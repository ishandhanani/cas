// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokenizers::Tokenizer;

use agent_loadgen_core::TraceRequest;

const CODEWORD_ALGORITHM: &str = "splitmix64-base-n-lsd-v2";
const SPECIAL_TOKEN_FIELDS: &[&str] = &[
    "bos_token",
    "eos_token",
    "pad_token",
    "sep_token",
    "cls_token",
    "mask_token",
];

#[derive(Debug, Clone, Serialize)]
pub struct SafeTokenAlphabet {
    token_ids: Vec<u32>,
    manifest: TokenAlphabetManifest,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenAlphabetManifest {
    pub source: String,
    pub verified: bool,
    pub tokenizer_digest_sha256: Option<String>,
    pub token_count: usize,
    pub minimum_token_id: u32,
    pub maximum_token_id: u32,
    pub excluded_token_count: usize,
    pub token_ids_digest_sha256: String,
}

#[derive(Debug, Clone)]
pub enum TokenAlphabetSource {
    Tokenizer(String),
    UnverifiedRange { start: u32, size: u32 },
}

impl SafeTokenAlphabet {
    pub fn load(
        source: TokenAlphabetSource,
        limit: usize,
        extra_excluded_ids: &[u32],
    ) -> Result<Self> {
        if limit < 2 {
            bail!("token alphabet size must be at least two");
        }
        match source {
            TokenAlphabetSource::Tokenizer(source) => {
                Self::from_tokenizer(&source, limit, extra_excluded_ids)
            }
            TokenAlphabetSource::UnverifiedRange { start, size } => {
                Self::from_unverified_range(start, size, extra_excluded_ids)
            }
        }
    }

    pub(crate) fn from_unverified_range(
        start: u32,
        size: u32,
        extra_excluded_ids: &[u32],
    ) -> Result<Self> {
        if size < 2 {
            bail!("token alphabet size must be at least two");
        }
        let end = start
            .checked_add(size)
            .context("token alphabet exceeds u32")?;
        let excluded = extra_excluded_ids.iter().copied().collect::<BTreeSet<_>>();
        let token_ids = (start..end)
            .filter(|token_id| !excluded.contains(token_id))
            .collect::<Vec<_>>();
        Self::finish(
            token_ids,
            format!("unverified-range:{start}:{size}"),
            false,
            None,
            excluded.len(),
        )
    }

    fn from_tokenizer(source: &str, limit: usize, extra_excluded_ids: &[u32]) -> Result<Self> {
        let files = TokenizerFiles::resolve(source)?;
        let tokenizer = Tokenizer::from_file(&files.tokenizer_json).map_err(|error| {
            anyhow::anyhow!(
                "failed to load tokenizer {}: {error}",
                files.tokenizer_json.display()
            )
        })?;

        let mut excluded = extra_excluded_ids.iter().copied().collect::<BTreeSet<_>>();
        for (token_id, token) in tokenizer.get_added_tokens_decoder() {
            if token.special {
                excluded.insert(token_id);
            }
        }
        for metadata in &files.metadata_json {
            collect_special_ids(metadata, &tokenizer, &mut excluded)?;
        }

        let mut token_ids = tokenizer
            .get_vocab(false)
            .into_values()
            .filter(|token_id| !excluded.contains(token_id))
            .collect::<Vec<_>>();
        token_ids.sort_unstable();
        token_ids.dedup();
        token_ids.truncate(limit);

        let tokenizer_bytes = fs::read(&files.tokenizer_json).with_context(|| {
            format!(
                "failed to read tokenizer {}",
                files.tokenizer_json.display()
            )
        })?;
        Self::finish(
            token_ids,
            files.source,
            true,
            Some(hex::encode(Sha256::digest(tokenizer_bytes))),
            excluded.len(),
        )
    }

    fn finish(
        token_ids: Vec<u32>,
        source: String,
        verified: bool,
        tokenizer_digest_sha256: Option<String>,
        excluded_token_count: usize,
    ) -> Result<Self> {
        if token_ids.len() < 2 {
            bail!("the safe token alphabet contains fewer than two token IDs");
        }
        let minimum_token_id = *token_ids.first().context("token alphabet is empty")?;
        let maximum_token_id = *token_ids.last().context("token alphabet is empty")?;
        let mut digest = Sha256::new();
        for token_id in &token_ids {
            digest.update(token_id.to_le_bytes());
        }
        let manifest = TokenAlphabetManifest {
            source,
            verified,
            tokenizer_digest_sha256,
            token_count: token_ids.len(),
            minimum_token_id,
            maximum_token_id,
            excluded_token_count,
            token_ids_digest_sha256: hex::encode(digest.finalize()),
        };
        Ok(Self {
            token_ids,
            manifest,
        })
    }

    pub fn manifest(&self) -> &TokenAlphabetManifest {
        &self.manifest
    }

    fn len(&self) -> usize {
        self.token_ids.len()
    }

    fn token(&self, digit: usize) -> u32 {
        self.token_ids[digit]
    }
}

#[derive(Debug)]
struct TokenizerFiles {
    source: String,
    tokenizer_json: PathBuf,
    metadata_json: Vec<PathBuf>,
}

impl TokenizerFiles {
    fn resolve(source: &str) -> Result<Self> {
        let path = Path::new(source);
        if path.is_file() {
            let directory = path.parent().unwrap_or_else(|| Path::new("."));
            return Ok(Self::local(source, path.to_path_buf(), directory));
        }
        if path.is_dir() {
            let tokenizer_json = path.join("tokenizer.json");
            if !tokenizer_json.is_file() {
                bail!(
                    "tokenizer directory {} has no tokenizer.json",
                    path.display()
                );
            }
            return Ok(Self::local(source, tokenizer_json, path));
        }

        let cache = hf_hub::Cache::default();
        let api = hf_hub::api::sync::ApiBuilder::from_cache(cache)
            .with_progress(false)
            .build()
            .map_err(|error| anyhow::anyhow!("failed to create Hugging Face client: {error}"))?;
        let repository = api.model(source.to_string());
        let tokenizer_json = repository.get("tokenizer.json").map_err(|error| {
            anyhow::anyhow!("failed to get tokenizer.json for {source:?}: {error}")
        })?;
        let mut metadata_json = Vec::new();
        for name in [
            "generation_config.json",
            "config.json",
            "tokenizer_config.json",
            "special_tokens_map.json",
        ] {
            if let Ok(path) = repository.get(name) {
                metadata_json.push(path);
            }
        }
        Ok(Self {
            source: format!("huggingface:{source}"),
            tokenizer_json,
            metadata_json,
        })
    }

    fn local(source: &str, tokenizer_json: PathBuf, directory: &Path) -> Self {
        let metadata_json = [
            "generation_config.json",
            "config.json",
            "tokenizer_config.json",
            "special_tokens_map.json",
        ]
        .into_iter()
        .map(|name| directory.join(name))
        .filter(|path| path.is_file())
        .collect();
        Self {
            source: format!("local:{source}"),
            tokenizer_json,
            metadata_json,
        }
    }
}

fn collect_special_ids(
    path: &Path,
    tokenizer: &Tokenizer,
    excluded: &mut BTreeSet<u32>,
) -> Result<()> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read tokenizer metadata {}", path.display()))?;
    let text = std::str::from_utf8(&bytes)
        .with_context(|| format!("tokenizer metadata is not UTF-8 in {}", path.display()))?;
    let value: serde_json::Value = json_five::from_str(text)
        .with_context(|| format!("invalid tokenizer metadata JSON in {}", path.display()))?;
    collect_named_integer_ids(&value, "bos_token_id", excluded);
    collect_named_integer_ids(&value, "eos_token_id", excluded);
    collect_named_integer_ids(&value, "pad_token_id", excluded);
    for field in SPECIAL_TOKEN_FIELDS {
        if let Some(token) = value.get(field).and_then(token_content)
            && let Some(token_id) = tokenizer.token_to_id(token)
        {
            excluded.insert(token_id);
        }
    }
    if let Some(decoder) = value
        .get("added_tokens_decoder")
        .and_then(serde_json::Value::as_object)
    {
        for (token_id, token) in decoder {
            if token.get("special").and_then(serde_json::Value::as_bool) == Some(true)
                && let Ok(token_id) = token_id.parse::<u32>()
            {
                excluded.insert(token_id);
            }
        }
    }
    Ok(())
}

fn collect_named_integer_ids(value: &serde_json::Value, field: &str, output: &mut BTreeSet<u32>) {
    if let Some(id) = value.get(field).and_then(serde_json::Value::as_u64)
        && let Ok(id) = u32::try_from(id)
    {
        output.insert(id);
    }
    if let Some(ids) = value.get(field).and_then(serde_json::Value::as_array) {
        output.extend(
            ids.iter()
                .filter_map(serde_json::Value::as_u64)
                .filter_map(|id| u32::try_from(id).ok()),
        );
    }
    if let Some(text_config) = value.get("text_config") {
        collect_named_integer_ids(text_config, field, output);
    }
}

fn token_content(value: &serde_json::Value) -> Option<&str> {
    value
        .as_str()
        .or_else(|| value.get("content").and_then(serde_json::Value::as_str))
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenDictionaryManifest {
    pub trace_block_size: usize,
    pub codeword_algorithm: &'static str,
    pub codeword_count: usize,
    pub alphabet: TokenAlphabetManifest,
    pub dictionary_digest_sha256: String,
}

#[derive(Debug, Clone)]
pub struct TokenDictionary {
    block_size: usize,
    alphabet: SafeTokenAlphabet,
    manifest: TokenDictionaryManifest,
}

impl TokenDictionary {
    pub fn new(
        block_size: usize,
        codeword_count: usize,
        alphabet: SafeTokenAlphabet,
    ) -> Result<Self> {
        if block_size == 0 {
            bail!("trace block size must be greater than zero");
        }
        validate_codeword_capacity(block_size, alphabet.len())?;

        let mut digest = Sha256::new();
        digest.update(CODEWORD_ALGORITHM.as_bytes());
        digest.update((block_size as u64).to_le_bytes());
        digest.update(alphabet.manifest().token_ids_digest_sha256.as_bytes());
        let manifest = TokenDictionaryManifest {
            trace_block_size: block_size,
            codeword_algorithm: CODEWORD_ALGORITHM,
            codeword_count,
            alphabet: alphabet.manifest().clone(),
            dictionary_digest_sha256: hex::encode(digest.finalize()),
        };
        Ok(Self {
            block_size,
            alphabet,
            manifest,
        })
    }

    #[cfg(test)]
    pub(crate) fn build(requests: &[TraceRequest], alphabet: SafeTokenAlphabet) -> Result<Self> {
        let block_size = requests
            .first()
            .context("cannot build a token dictionary for an empty trace")?
            .trace_block_size;
        if requests
            .iter()
            .any(|request| request.trace_block_size != block_size)
        {
            bail!("all requests must use one trace block size");
        }
        let codeword_count = requests
            .iter()
            .flat_map(|request| request.input_sequence_hashes.iter().copied())
            .collect::<BTreeSet<_>>()
            .len();
        Self::new(block_size, codeword_count, alphabet)
    }

    pub fn manifest(&self) -> &TokenDictionaryManifest {
        &self.manifest
    }

    pub fn synthesize(&self, request: &TraceRequest) -> Result<Vec<u32>> {
        if request.trace_block_size != self.block_size {
            bail!("request block size does not match the token dictionary");
        }
        let mut tokens = Vec::with_capacity(request.input_tokens);
        for hash in &request.input_sequence_hashes {
            let codeword = make_codeword(*hash, self.block_size, &self.alphabet);
            let remaining = request.input_tokens - tokens.len();
            tokens.extend_from_slice(&codeword[..remaining.min(self.block_size)]);
            if tokens.len() == request.input_tokens {
                break;
            }
        }
        if tokens.len() != request.input_tokens {
            bail!(
                "request {} produced {} tokens, expected {}",
                request.source_request_id,
                tokens.len(),
                request.input_tokens
            );
        }
        Ok(tokens)
    }
}

fn validate_codeword_capacity(block_size: usize, alphabet_size: usize) -> Result<()> {
    let mut capacity = 1_u128;
    for _ in 0..block_size {
        capacity = capacity.saturating_mul(alphabet_size as u128);
    }
    if capacity <= u64::MAX as u128 {
        bail!(
            "a token alphabet of {alphabet_size} IDs and block size {block_size} cannot encode every u64 trace hash without collisions"
        );
    }
    Ok(())
}

fn make_codeword(hash: u64, block_size: usize, alphabet: &SafeTokenAlphabet) -> Vec<u32> {
    let base = alphabet.len() as u64;
    let mut value = splitmix64(hash);
    let mut digits = vec![0_usize; block_size];
    // Put the least-significant digits first. A u64 needs only seven base-1024
    // digits, while a normal KV block has 16 or more tokens. Big-endian digits
    // would therefore make the beginning of every short final block zero and
    // collapse unrelated partial blocks to the same dummy-token prefix.
    for digit in &mut digits {
        *digit = (value % base) as usize;
        value /= base;
    }
    debug_assert_eq!(value, 0);
    digits
        .into_iter()
        .map(|digit| alphabet.token(digit))
        .collect()
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    fn request(id: &str, input_tokens: usize, hashes: &[u64]) -> TraceRequest {
        TraceRequest {
            ordinal: 0,
            source_request_id: id.to_string(),
            source_x_request_id: None,
            source_model: None,
            input_tokens,
            output_tokens: 2,
            request_received_ms: 1,
            trace_block_size: 4,
            input_sequence_hashes: hashes.to_vec(),
            agent_context: None,
        }
    }

    fn alphabet() -> SafeTokenAlphabet {
        SafeTokenAlphabet::from_unverified_range(100, 65_536, &[]).unwrap()
    }

    #[test]
    fn preserves_shared_prefix_and_exact_length() {
        let first = request("a", 6, &[11, 22]);
        let second = request("b", 8, &[11, 33]);
        let dictionary =
            TokenDictionary::build(&[first.clone(), second.clone()], alphabet()).unwrap();
        let first_tokens = dictionary.synthesize(&first).unwrap();
        let second_tokens = dictionary.synthesize(&second).unwrap();

        assert_eq!(first_tokens.len(), 6);
        assert_eq!(second_tokens.len(), 8);
        assert_eq!(&first_tokens[..4], &second_tokens[..4]);
        assert_ne!(&first_tokens[4..], &second_tokens[4..6]);
    }

    #[test]
    fn injective_codewords_cover_extreme_hashes() {
        let alphabet = alphabet();
        let low = make_codeword(0, 4, &alphabet);
        let high = make_codeword(u64::MAX, 4, &alphabet);
        assert_ne!(low, high);
        assert_eq!(low.len(), 4);
        assert_eq!(high.len(), 4);
    }

    #[test]
    fn partial_blocks_use_hash_entropy_from_the_first_token() {
        let alphabet = SafeTokenAlphabet::from_unverified_range(100, 1024, &[]).unwrap();
        let first = make_codeword(11, 16, &alphabet);
        let second = make_codeword(22, 16, &alphabet);

        assert_ne!(first[0], second[0]);
        assert_ne!(&first[..4], &second[..4]);
    }

    #[test]
    fn rejects_insufficient_codeword_capacity() {
        let alphabet = SafeTokenAlphabet::from_unverified_range(0, 2, &[]).unwrap();
        assert!(TokenDictionary::new(16, 1, alphabet).is_err());
    }

    #[test]
    fn tokenizer_alphabet_excludes_special_tokens() {
        let directory = TempDir::new().unwrap();
        let tokenizer = directory.path().join("tokenizer.json");
        let mut file = fs::File::create(&tokenizer).unwrap();
        write!(
            file,
            "{}",
            serde_json::json!({
                "version": "1.0",
                "truncation": null,
                "padding": null,
                "added_tokens": [
                    {"id": 3, "content": "[EOS]", "single_word": false, "lstrip": false, "rstrip": false, "normalized": false, "special": true}
                ],
                "normalizer": null,
                "pre_tokenizer": {"type": "Whitespace"},
                "post_processor": null,
                "decoder": null,
                "model": {"type": "WordLevel", "vocab": {"a": 0, "b": 1, "c": 2, "[EOS]": 3}, "unk_token": "a"}
            })
        )
        .unwrap();
        fs::write(
            directory.path().join("generation_config.json"),
            r#"{"eos_token_id":3}"#,
        )
        .unwrap();

        let alphabet = SafeTokenAlphabet::load(
            TokenAlphabetSource::Tokenizer(directory.path().display().to_string()),
            16,
            &[],
        )
        .unwrap();
        assert_eq!(alphabet.token_ids, vec![0, 1, 2]);
        assert!(alphabet.manifest.verified);
        assert_eq!(alphabet.manifest.excluded_token_count, 1);
    }
}
