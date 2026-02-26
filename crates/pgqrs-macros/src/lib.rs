use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::Parse, parse_macro_input, punctuated::Punctuated, token::Comma, Error, Expr, ExprLit,
    FnArg, GenericArgument, Ident, ItemFn, Lit, LitStr, Meta, PathArguments, ReturnType, Type,
};

struct WorkflowArgs {
    metas: Vec<Meta>,
}

impl Parse for WorkflowArgs {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let punct: Punctuated<Meta, Comma> = Punctuated::parse_terminated(input)?;
        Ok(Self {
            metas: punct.into_iter().collect(),
        })
    }
}

/// Attribute macro for defining a **workflow step** with automatic retry / resume semantics.
///
/// # Overview
///
/// `#[pgqrs_step]` wraps a step function so that it:
///
/// - Derives a **step name** from the function name.
/// - Uses `pgqrs::Run` to:
///   - Skip execution if the step has already completed successfully.
///   - Run the step body when needed.
///   - Persist the step result as **success** or **failure**.
/// - Automatically resumes correctly on retries by consulting stored step state.
///
/// This macro is intended to be used on individual steps inside a workflow, typically
/// alongside [`pgqrs_workflow`] on the workflow entry point.
///
/// # Signature requirements
///
/// The macro expects the annotated function to follow these rules:
///
/// 1. **First argument is a workflow/context value**
///    - The function must take **at least one argument**.
///    - The first argument must be a **named** pattern, e.g. `ctx` or `workflow`.
///    - That context is used to construct the step guard via:
///      `ctx.acquire_step(step_name, current_time).await?`.
///    - Concretely, the context type is expected to implement `pgqrs::Run`.
///
///    If the first argument is missing or is not a simple identifier pattern,
///    compilation will fail with a descriptive error.
///
/// 2. **Return type must be a `Result`**
///    - The annotated function must return `Result<T, E>` (for some `T` and `E`).
///    - On success (`Ok(value)`), the wrapper calls:
///      `guard.success(value).await?`.
///    - On failure (`Err(err)`), the wrapper calls:
///      `guard.fail(err.to_string()).await?`.
///
///    The macro does not enforce the concrete `T` or `E` types, but it assumes the
///    function returns a `Result`-like value it can pattern-match as `Ok` / `Err`.
///
/// 3. **Async usage**
///    - The generated wrapper uses `.await` on `acquire_step` and on the
///      `success` / `fail` methods, so the annotated function is typically
///      declared as `async fn`.
///
/// # Step name derivation
///
/// The step identifier used by the workflow engine is derived from the function
/// name:
///
/// ```rust,ignore
/// let fn_name = &input_fn.sig.ident;
/// let step_name = fn_name.to_string(); // step name is the function name
/// ```
///
/// This means that renaming a step function will also change its step name, which
/// can affect how previously persisted step state is associated. Choose stable
/// and descriptive names for step functions.
///
/// # Runtime behavior
///
/// At runtime, the generated wrapper:
///
/// 1. Constructs a step guard:
///    `ctx.acquire_step(step_name).await?`.
/// 2. Examines the returned step result:
///    - `StepResult::Skipped(val)` — the step has already completed; the stored
///      value is returned immediately as `Ok(val)` without re-running the body.
///    - `StepResult::Execute(guard)` — the step body should be executed now.
/// 3. When executing, it runs the original body in an async block, obtains the
///    `Result<T, E>`, and:
///    - Calls `guard.success(value).await?` on `Ok(value)`.
///    - Calls `guard.fail(err.to_string()).await?` on `Err(err)`.
/// 4. Finally, it returns the original `Result<T, E>` from the step function.
///
/// This behavior enables automatic retry and resume semantics for each annotated
/// step function.
///
/// # Examples
///
/// A minimal workflow context type might look like:
///
/// ```rust,ignore
/// use pgqrs::Run;
///
/// // Workflow struct provided by pgqrs already implements Run.
/// // pub struct Workflow { ... }
/// ```
///
/// A typical step function:
///
/// ```rust,ignore
/// use pgqrs_macros::pgqrs_step;
/// use pgqrs::Run;
///
/// #[pgqrs_step]
/// pub async fn charge_customer(
///     ctx: &Run,
///     amount_cents: i64,
/// ) -> Result<(), anyhow::Error> {
///     // This body is wrapped with StepGuard logic:
///     // - On first execution: run the code, persist success/failure.
///     // - On retry: skip if already successful.
///     // ctx.pool() and ctx.id() are accessible.
///     charge_service::charge(ctx.pool(), ctx.id(), amount_cents).await?;
///     Ok(())
/// }
/// ```
///
/// The step ID for the example above will be `"charge_customer"`.
#[proc_macro_attribute]
pub fn pgqrs_step(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let _step_name = fn_name.to_string(); // Default to fn name
    let attrs = &input_fn.attrs;

    // Identify the first argument name to use as context (e.g., ctx)
    let first_arg_name = if let Some(syn::FnArg::Typed(pat_type)) = input_fn.sig.inputs.first() {
        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
            &pat_ident.ident
        } else {
            return syn::Error::new_spanned(
                pat_type,
                "First argument must be a named parameter of type &Run (e.g., ctx: &Run)",
            )
            .to_compile_error()
            .into();
        }
    } else {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "Step function must take at least one argument (context)",
        )
        .to_compile_error()
        .into();
    };

    let block = &input_fn.block;
    let visibility = &input_fn.vis;
    let sig = &input_fn.sig;

    // Validate return type is Result
    let output_type = match &sig.output {
        syn::ReturnType::Type(_, ty) => {
            // Simple heuristic to check if return type is a Result.
            // Checks if the last path segment is "Result".
            let is_result = if let syn::Type::Path(type_path) = &**ty {
                type_path
                    .path
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "Result")
            } else {
                false
            };

            if !is_result {
                return syn::Error::new_spanned(
                    &sig.output,
                    "Step function must return a Result type (e.g. Result<T, E>)",
                )
                .to_compile_error()
                .into();
            }
            quote! { #ty }
        }
        syn::ReturnType::Default => {
            return syn::Error::new_spanned(
                &sig.output,
                "Step function must return a Result type (e.g. Result<T, E>)",
            )
            .to_compile_error()
            .into();
        }
    };

    let expanded = quote! {
        #(#attrs)*
        #visibility #sig {
            let result: #output_type = match pgqrs::workflow_step(#first_arg_name, stringify!(#fn_name), || async {
                let result: #output_type = async { #block }.await;
                match result {
                    Ok(value) => Ok(value),
                    Err(err) => Err(pgqrs::Error::Internal {
                        message: err.to_string(),
                    }),
                }
            })
            .await
            {
                Ok(value) => Ok(value),
                Err(err) => match err {
                    pgqrs::Error::Internal { message } => Err(anyhow::anyhow!(message)),
                    other => Err(anyhow::Error::new(other)),
                },
            };

            result
        }
    };

    TokenStream::from(expanded)
}

/// Attribute macro for defining a **workflow entry point**.
///
/// # Overview
///
/// `#[pgqrs_workflow]` marks a function as the entry point of a durable workflow.
/// It wraps the function to:
///
/// - Start the workflow execution (transition from `PENDING` to `RUNNING`).
/// - Execute the workflow logic.
/// - Mark the workflow as `SUCCESS` or `ERROR` upon completion.
///
/// # Signature requirements
///
/// 1. **First argument is a workflow handle**
///    - The function must take at least one argument.
///    - The first argument must be a named pattern typed as `&Run` (or similar context).
///    - This handle is used to call `.start()`, `.success()`, and `.fail()`.
///
/// 2. **Return type must be a `Result`**
///    - On `Ok(value)`, the workflow is marked as `SUCCESS` with the serialized value.
///    - On `Err(err)`, the workflow is marked as `ERROR` with the serialized error string.
///
/// 3. **Async usage**
///    - The function must be `async fn`.
///
/// # Usage
///
/// Typically, you create the workflow row **before** calling this function using
/// `Workflow::create`. This separation allows the ID to be generated by the database
/// and returned to the caller immediately, while the execution happens asynchronously.
///
/// # Example
///
/// ```rust,ignore
/// use pgqrs::Run;
/// use pgqrs_macros::pgqrs_workflow;
///
/// #[pgqrs_workflow]
/// async fn process_order(workflow: &Run, order_id: i32) -> Result<String, anyhow::Error> {
///     // Workflow logic here...
///     Ok("Order processed".to_string())
/// }
///
/// // Usage:
/// // let workflow = Workflow::create(pool, "process_order", &order_id).await?;
/// // process_order(&workflow, order_id).await?;
/// ```
#[proc_macro_attribute]
pub fn pgqrs_workflow(attr_args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr_args as WorkflowArgs);
    let mut workflow_fn = parse_macro_input!(item as ItemFn);
    workflow_fn
        .attrs
        .retain(|attr| !attr.path().is_ident("pgqrs_workflow"));

    let fn_name = workflow_fn.sig.ident.clone();
    let workflow_name = match resolve_workflow_name(&args.metas, &fn_name) {
        Ok(name) => name,
        Err(err) => return err.to_compile_error().into(),
    };

    if workflow_fn.sig.inputs.is_empty() {
        return Error::new_spanned(
            &workflow_fn.sig.inputs,
            "Workflow functions must take at least one workflow handle argument",
        )
        .to_compile_error()
        .into();
    }

    if workflow_fn.sig.inputs.len() > 2 {
        return Error::new_spanned(
            &workflow_fn.sig.inputs,
            "Workflow functions must take at most one workflow handle and one optional payload argument",
        )
        .to_compile_error()
        .into();
    }

    let data_ty = if workflow_fn.sig.inputs.len() == 2 {
        let data_arg = workflow_fn.sig.inputs.iter().nth(1).unwrap();
        match data_arg {
            FnArg::Typed(pat_type) => (*pat_type.ty).clone(),
            FnArg::Receiver(_) => {
                return Error::new_spanned(data_arg, "Workflow payload argument must be typed")
                    .to_compile_error()
                    .into();
            }
        }
    } else {
        // Default to unit type () when no payload is provided
        syn::parse_quote!(())
    };

    let output_type = match extract_workflow_output(&workflow_fn.sig.output) {
        Ok(ty) => ty,
        Err(err) => return err.to_compile_error().into(),
    };

    let inner_ident = format_ident!("{}_inner", fn_name);
    let runner_ident = format_ident!("{}_runner", fn_name);
    let visibility = workflow_fn.vis.clone();
    workflow_fn.sig.ident = inner_ident.clone();

    // Determine how to call the inner function based on the number of arguments
    let call_inner = if workflow_fn.sig.inputs.len() == 2 {
        quote! { #inner_ident(&run, input).await }
    } else {
        quote! { #inner_ident(&run).await }
    };

    let expanded = quote! {
        #workflow_fn

        fn #runner_ident(run: Run, input: #data_ty) -> pgqrs::WorkflowFuture<#output_type> {
            Box::pin(async move {
                match #call_inner {
                    Ok(value) => Ok(value),
                    Err(err) => {
                        // Preserve special error types, wrap others in Internal
                        // Convert any error to anyhow::Error, then attempt downcast
                        let anyhow_err: anyhow::Error = err.into();
                        match anyhow_err.downcast::<pgqrs::Error>() {
                            Ok(pgqrs_err) => Err(pgqrs_err),
                            Err(e) => Err(pgqrs::Error::Internal {
                                message: e.to_string(),
                            }),
                        }
                    }
                }
            })
        }

        #[allow(non_upper_case_globals)]
        #visibility const #fn_name: pgqrs::WorkflowDef<#data_ty, #output_type> =
            pgqrs::WorkflowDef::new(#workflow_name, #runner_ident);
    };

    TokenStream::from(expanded)
}

fn resolve_workflow_name(args: &[syn::Meta], default: &Ident) -> Result<LitStr, Error> {
    let mut name: Option<LitStr> = None;

    for meta in args {
        match meta {
            Meta::NameValue(nv) if nv.path.is_ident("name") => {
                let litstr = match &nv.value {
                    Expr::Lit(ExprLit {
                        lit: Lit::Str(litstr),
                        ..
                    }) => litstr.clone(),
                    other => {
                        return Err(Error::new_spanned(
                            other,
                            "`name` argument must be a string literal",
                        ));
                    }
                };

                if name.is_some() {
                    return Err(Error::new_spanned(nv, "Duplicate `name` argument"));
                }

                name = Some(litstr);
            }
            other => {
                return Err(Error::new_spanned(
                    other,
                    "Unexpected attribute argument: expected `name = \"...\"`",
                ));
            }
        }
    }

    Ok(name.unwrap_or_else(|| LitStr::new(&default.to_string(), default.span())))
}

fn extract_workflow_output(output: &ReturnType) -> Result<Type, Error> {
    match output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(type_path) = &**ty {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident != "Result" {
                        return Err(Error::new_spanned(
                            output,
                            "Workflow function must return `Result<Output, Error>`",
                        ));
                    }

                    if let PathArguments::AngleBracketed(angle) = &segment.arguments {
                        if let Some(GenericArgument::Type(output_ty)) = angle.args.first() {
                            return Ok(output_ty.clone());
                        }
                    }
                }
            }

            Err(Error::new_spanned(
                output,
                "Workflow function must return `Result<Output, Error>`",
            ))
        }
        ReturnType::Default => Err(Error::new_spanned(
            output,
            "Workflow function must return a Result type",
        )),
    }
}
