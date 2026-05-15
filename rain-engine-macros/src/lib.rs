//! Procedural macros for RainEngine skill manifests.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    DeriveInput, LitInt, LitStr, Meta, Token, parse::Parser, parse_macro_input,
    punctuated::Punctuated,
};

#[proc_macro_derive(SkillManifest, attributes(skill))]
pub fn derive_skill_manifest(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;

    let mut name = None::<LitStr>;
    let mut description = None::<LitStr>;
    let mut timeout_ms = None::<LitInt>;
    let mut max_memory_bytes = None::<LitInt>;
    let mut max_fuel = None::<LitInt>;
    let mut approval_required = false;
    let mut scopes = Vec::<LitStr>::new();
    let mut capabilities = Vec::<LitStr>::new();

    for attr in &input.attrs {
        if !attr.path().is_ident("skill") {
            continue;
        }
        let metas = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .expect("invalid #[skill(...)] attribute");
        for meta in metas {
            match meta {
                Meta::NameValue(value) if value.path.is_ident("name") => {
                    if let syn::Expr::Lit(expr) = value.value
                        && let syn::Lit::Str(lit) = expr.lit
                    {
                        name = Some(lit);
                    }
                }
                Meta::NameValue(value) if value.path.is_ident("description") => {
                    if let syn::Expr::Lit(expr) = value.value
                        && let syn::Lit::Str(lit) = expr.lit
                    {
                        description = Some(lit);
                    }
                }
                Meta::NameValue(value) if value.path.is_ident("timeout_ms") => {
                    if let syn::Expr::Lit(expr) = value.value
                        && let syn::Lit::Int(lit) = expr.lit
                    {
                        timeout_ms = Some(lit);
                    }
                }
                Meta::NameValue(value) if value.path.is_ident("max_memory_bytes") => {
                    if let syn::Expr::Lit(expr) = value.value
                        && let syn::Lit::Int(lit) = expr.lit
                    {
                        max_memory_bytes = Some(lit);
                    }
                }
                Meta::NameValue(value) if value.path.is_ident("max_fuel") => {
                    if let syn::Expr::Lit(expr) = value.value
                        && let syn::Lit::Int(lit) = expr.lit
                    {
                        max_fuel = Some(lit);
                    }
                }
                Meta::NameValue(value) if value.path.is_ident("approval_required") => {
                    if let syn::Expr::Lit(expr) = value.value
                        && let syn::Lit::Bool(lit) = expr.lit
                    {
                        approval_required = lit.value;
                    }
                }
                Meta::List(list) if list.path.is_ident("scopes") => {
                    let parser = Punctuated::<LitStr, Token![,]>::parse_terminated;
                    scopes.extend(parser.parse2(list.tokens).expect("invalid scopes"));
                }
                Meta::List(list) if list.path.is_ident("capabilities") => {
                    let parser = Punctuated::<LitStr, Token![,]>::parse_terminated;
                    capabilities.extend(parser.parse2(list.tokens).expect("invalid capabilities"));
                }
                _ => {}
            }
        }
    }

    let name = name.expect("skill name is required");
    let description = description.expect("skill description is required");
    let timeout_ms = timeout_ms.unwrap_or_else(|| LitInt::new("5000", name.span()));
    let max_memory_bytes = max_memory_bytes.unwrap_or_else(|| LitInt::new("8388608", name.span()));
    let max_fuel_tokens = if let Some(max_fuel) = max_fuel {
        quote! { Some(#max_fuel) }
    } else {
        quote! { None }
    };

    let scope_tokens = scopes
        .into_iter()
        .map(|scope| quote! { #scope.to_string() });
    let capability_tokens = capabilities.into_iter().map(parse_capability);

    TokenStream::from(quote! {
        impl rain_engine_core::SkillManifestDescriptor for #ident {
            fn skill_manifest() -> rain_engine_core::SkillManifest {
                let schema = schemars::schema_for!(#ident);
                rain_engine_core::SkillManifest {
                    name: #name.to_string(),
                    description: #description.to_string(),
                    input_schema: serde_json::to_value(schema).expect("schema serializes"),
                    required_scopes: vec![#(#scope_tokens),*],
                    capability_grants: vec![#(#capability_tokens),*],
                    resource_policy: rain_engine_core::ResourcePolicy {
                        timeout_ms: #timeout_ms,
                        max_memory_bytes: #max_memory_bytes,
                        max_fuel: #max_fuel_tokens,
                    },
                    approval_required: #approval_required,
                }
            }
        }
    })
}

fn parse_capability(value: LitStr) -> proc_macro2::TokenStream {
    let raw = value.value();
    if raw == "log" {
        quote! { rain_engine_core::SkillCapability::StructuredLog }
    } else if let Some(namespace) = raw.strip_prefix("kv:") {
        quote! {
            rain_engine_core::SkillCapability::KeyValueRead {
                namespaces: vec![#namespace.to_string()],
            }
        }
    } else if let Some(host) = raw.strip_prefix("http:") {
        quote! {
            rain_engine_core::SkillCapability::HttpOutbound {
                allow_hosts: vec![#host.to_string()],
            }
        }
    } else {
        panic!("unsupported capability `{raw}`");
    }
}
