use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, DeriveInput, Type};

#[proc_macro_derive(Message, attributes(response))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Specify Type explicitly with parse_args::<Type>()
    let response_type = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("response"))
        .map(|attr| {
            attr.parse_args::<Type>()
                .expect("Expected #[response(Type)]")
        })
        .unwrap_or_else(|| parse_quote!(()));

    quote! {
        impl Message for #name {
            type Response = #response_type;
        }
    }
    .into()
}
