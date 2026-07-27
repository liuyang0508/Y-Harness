//! Protocol transports kept separate from provider and runtime semantics.

mod mcp;
#[cfg(feature = "https-mcp")]
mod mcp_https;
#[cfg(feature = "tls-host")]
mod tls_jsonl;

pub use mcp::{
    McpClient, McpToolDescriptor, StdioMcpClient, StdioMcpConfig, StdioMcpLaunchAuthority,
    mcp_client, register_mcp_tools, register_selected_mcp_tools,
};
#[cfg(feature = "https-mcp")]
pub use mcp_https::{HttpsJsonMcpClient, HttpsJsonMcpConfig};
#[cfg(feature = "tls-host")]
pub use tls_jsonl::{TlsJsonlServer, TlsJsonlServerConfig, TlsJsonlServerReport};
