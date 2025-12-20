use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Attribute macro for defining a **workflow step** with automatic retry / resume semantics.
///
/// # Overview
///
/// `#[pgqrs_step]` wraps a step function so that it:
///
/// - Derives a **step ID** from the function name.
/// - Uses [`pgqrs::workflow::StepGuard`] to:
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
///    - That context is used to construct the step guard:
///      `StepGuard::new(&ctx.pool, ctx.id, step_id)`.
///    - Concretely, the context type is expected to expose:
///      - A `pool` field (used as a database / storage handle).
///      - An `id` field (the workflow instance identifier).
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
///    - The generated wrapper uses `.await` on `StepGuard::new` and on the
///      `success` / `fail` methods, so the annotated function is typically
///      declared as `async fn`.
///
/// # Step ID derivation
///
/// The step identifier used by the workflow engine is derived from the function
/// name:
///
/// ```rust
/// let fn_name = &input_fn.sig.ident;
/// let step_id = fn_name.to_string(); // step ID is the function name
/// ```
///
/// This means that renaming a step function will also change its step ID, which
/// can affect how previously persisted step state is associated. Choose stable
/// and descriptive names for step functions.
///
/// # Runtime behavior
///
/// At runtime, the generated wrapper:
///
/// 1. Constructs a step guard:
///    `StepGuard::new(&ctx.pool, ctx.id, step_id).await?`.
/// 2. Examines the returned [`pgqrs::workflow::StepResult`]:
///    - `StepResult::Skipped(val)` — the step has already completed; the stored
///      value is returned immediately as `Ok(val)` without re-running the body.
///    - `StepResult::Execute(guard)` — the step body should be executed now.
/// 3. When executing, it runs the original body in an async block, obtains the
///    `Result<T, E>`, and:
///    - Calls `guard.success(&value).await?` on `Ok(value)`.
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
/// use pgqrs::WorkflowId;
///
/// pub struct WorkflowCtx {
///     pub pool: pgqrs::DbPool,
///     pub id: WorkflowId,
/// }
/// ```
///
/// A typical step function:
///
/// ```rust,ignore
/// use pgqrs_macros::pgqrs_step;
///
/// #[pgqrs_step]
/// pub async fn charge_customer(
///     ctx: &WorkflowCtx,
///     amount_cents: i64,
/// ) -> Result<(), anyhow::Error> {
///     // This body is wrapped with StepGuard logic:
///     // - On first execution: run the code, persist success/failure.
///     // - On retry: skip if already successful.
///     ctx.pool.charge(ctx.id, amount_cents).await?;
///     Ok(())
/// }
/// ```
///
/// The step ID for the example above will be `"charge_customer"`.
#[proc_macro_attribute]
pub fn pgqrs_step(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let step_id = fn_name.to_string(); // Default to fn name

    // Identify the first argument name to use as context (e.g., ctx)
    let first_arg_name = if let Some(syn::FnArg::Typed(pat_type)) = input_fn.sig.inputs.first() {
        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
            &pat_ident.ident
        } else {
            return syn::Error::new_spanned(
                pat_type,
                "First argument must be a named context identifier (e.g. ctx)",
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

    let output_type = match &sig.output {
        syn::ReturnType::Default => quote! { () },
        syn::ReturnType::Type(_, ty) => quote! { #ty },
    };

    let expanded = quote! {
        #visibility #sig {
            let step_id = #step_id;

            // Attempt to initialize the step
            let guard_res = pgqrs::workflow::StepGuard::acquire(#first_arg_name.pool(), #first_arg_name.id(), step_id)
                .await?;

            match guard_res {
                pgqrs::workflow::StepResult::Skipped(val) => Ok(val),
                pgqrs::workflow::StepResult::Execute(guard) => {
                    // Execute the original function body and capture the result
                    let result: #output_type = #block;

                    match &result {
                        Ok(val) => guard.success(val).await?,
                        Err(e) => {
                            // Serialize the error in a simple structured format to preserve details
                            let message = e.to_string();
                            let escaped = message.replace('"', "\\\"");
                            let structured_error = format!("{{\"error\":\"{}\"}}", escaped);
                            guard.fail(structured_error).await?
                        },
                    }

                    result
                }
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn pgqrs_workflow(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let workflow_name = fn_name.to_string();

    // Identify first argument (workflow context)
    let first_arg_name = if let Some(syn::FnArg::Typed(pat_type)) = input_fn.sig.inputs.first() {
        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
            &pat_ident.ident
        } else {
            return syn::Error::new_spanned(
                pat_type,
                "First argument must be a named workflow identifier (e.g. workflow)",
            )
            .to_compile_error()
            .into();
        }
    } else {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "Workflow function must take at least one argument (workflow context)",
        )
        .to_compile_error()
        .into();
    };

    // Identify second argument (input) for logging, defaults to "null" if missing
    let input_arg = if input_fn.sig.inputs.len() >= 2 {
        if let Some(syn::FnArg::Typed(pat_type)) = input_fn.sig.inputs.iter().nth(1) {
            if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                let ident = &pat_ident.ident;
                quote! { #ident }
            } else {
                quote! { &() }
            }
        } else {
            quote! { &() }
        }
    } else {
        quote! { &() }
    };

    let block = &input_fn.block;
    let visibility = &input_fn.vis;
    let sig = &input_fn.sig;

    let output_type = match &sig.output {
        syn::ReturnType::Default => quote! { () },
        syn::ReturnType::Type(_, ty) => quote! { #ty },
    };

    let expanded = quote! {
        #visibility #sig {
            let workflow_name = #workflow_name;

            // Start workflow
            #first_arg_name.start(workflow_name, #input_arg).await?;

            // Execute body
            let result: #output_type = async { #block }.await;

            // Handle terminal state
            match &result {
                Ok(val) => #first_arg_name.success(val).await?,
                Err(e) => #first_arg_name.fail(e.to_string()).await?,
            }

            result
        }
    };

    TokenStream::from(expanded)
}
