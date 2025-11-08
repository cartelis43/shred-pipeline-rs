// Shared types for the Shred Pipeline.
//
// These are intentionally lightweight, serializable-friendly plain data structs that
// represent decoded transactions, account information, tags and instruction metadata.
// Adjust fields to match the real Shred Pipeline / Solana domain model as needed.

use std::collections::HashMap;

/// Simple key/value tags attached to decoded items (transactions, entries, etc.).
/// Represented as an ordered list of (key, value) pairs to preserve insertion order
/// while staying small and dependency free.
pub type Tags = Vec<(String, String)>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenAccountInfo {
    /// Account pubkey (base58 or hex depending on your infra)
    pub pubkey: String,

    /// Lamports in the account
    pub lamports: u64,

    /// Owner program id (e.g. "Tokenkeg...") as a string
    pub owner: String,

    /// Raw account data
    pub data: Vec<u8>,

    /// Optional SPL token amount (if this is a token account)
    pub token_amount: Option<u64>,

    /// Optional extra metadata (rent epoch, delegates, etc.)
    pub meta: HashMap<String, String>,
}

impl Default for TokenAccountInfo {
    fn default() -> Self {
        Self {
            pubkey: Default::default(),
            lamports: 0,
            owner: Default::default(),
            data: Vec::new(),
            token_amount: None,
            meta: HashMap::new(),
        }
    }
}

/// A single instruction decoded from a transaction.
///
/// - `program_id` is the program that will be invoked.
/// - `accounts` is the list of account pubkeys referenced by the instruction (in order).
/// - `data` is the raw instruction data payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Instruction {
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data: Vec<u8>,
}

/// A decoded transaction produced by the Decoder layer.
///
/// This struct contains the minimal commonly-used fields for downstream processing:
/// signatures, slot/index origin, execution result, fees, decoded instructions,
/// referenced account infos and optional tags/annotations produced by the decoder.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecodedTx {
    /// Primary/minimal transaction identifier (e.g. first signature as base58).
    pub signature: String,

    /// Slot this transaction originated from (if known)
    pub slot: Option<u64>,

    /// Shred/index position within the slot (if available)
    pub index: Option<u32>,

    /// Whether the transaction was successfully executed (if known)
    pub success: Option<bool>,

    /// Fee paid for the transaction (if parsed)
    pub fee: Option<u64>,

    /// Decoded instructions (may be empty if not decoded)
    pub instructions: Vec<Instruction>,

    /// Accounts touched / referenced by the transaction
    pub accounts: Vec<TokenAccountInfo>,

    /// Arbitrary tags/annotations created by the decoder (e.g. "transfer", "stake", "vote")
    pub tags: Tags,

    /// Raw transaction bytes (wire format) for debugging or re-parsing
    pub raw_tx: Vec<u8>,

    /// Any extra metadata the decoder wants to attach
    pub meta: HashMap<String, String>,
}
