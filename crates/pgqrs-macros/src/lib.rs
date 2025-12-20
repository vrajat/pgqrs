use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

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
             return syn::Error::new_spanned(pat_type, "First argument must be a named context identifier (e.g. ctx)").to_compile_error().into();
        }
    } else {
         return syn::Error::new_spanned(&input_fn.sig, "Step function must take at least one argument (context)").to_compile_error().into();
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
            let guard_res = pgqrs::workflow::StepGuard::new(&#first_arg_name.pool, #first_arg_name.id, step_id)
                .await?;

            match guard_res {
                pgqrs::workflow::StepResult::Skipped(val) => Ok(val),
                pgqrs::workflow::StepResult::Execute(guard) => {
                    // Wrap the original function body in an async block to capture the result
                    let result: #output_type = async { #block }.await;

                    match &result {
                        Ok(val) => guard.success(val).await?,
                        Err(e) => guard.fail(e.to_string()).await?,
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
    // Currently a pass-through, but reserved for future injection or validation
    input
}
