// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::trace::TraceRequest;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SafeTokenAlphabet {
    pub start: u32,
    pub size: u32,
}

impl SafeTokenAlphabet {
    pub fn new(start: u32, size: u32) -> Result<Self> {
        if size < 2 {
            bail!("token alphabet size must be at least two");
        }
        start
            .checked_add(size - 1)
            .context("token alphabet exceeds u32")?;
        Ok(Self { start, size })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenDictionaryManifest {
    pub trace_block_size: usize,
    pub token_start: u32,
    pub token_alphabet_size: u32,
    pub codeword_count: usize,
    pub dictionary_digest_sha256: String,
}

#[derive(Debug, Clone)]
pub struct TokenDictionary {
    block_size: usize,
    alphabet: SafeTokenAlphabet,
    codewords: BTreeMap<u64, Vec<u32>>,
    manifest: TokenDictionaryManifest,
}

impl TokenDictionary {
    pub fn build(requests: &[TraceRequest], alphabet: SafeTokenAlphabet) -> Result<Self> {
        let block_size = requests
            .first()
            .context("cannot build a token dictionary for an empty trace")?
            .trace_block_size;
        if block_size == 0 {
            bail!("trace block size must be greater than zero");
        }
        if requests
            .iter()
            .any(|request| request.trace_block_size != block_size)
        {
            bail!("all requests must use one trace block size");
        }

        let hashes: BTreeSet<u64> = requests
            .iter()
            .flat_map(|request| request.input_sequence_hashes.iter().copied())
            .collect();
        let mut codewords = BTreeMap::new();
        let mut owners: HashMap<Vec<u32>, u64> = HashMap::new();
        for hash in hashes {
            let mut nonce = 0_u64;
            let codeword = loop {
                let candidate = make_codeword(hash, nonce, block_size, alphabet);
                match owners.get(&candidate) {
                    None => break candidate,
                    Some(owner) if *owner == hash => break candidate,
                    Some(_) => nonce = nonce.checked_add(1).context("codeword nonce overflow")?,
                }
            };
            owners.insert(codeword.clone(), hash);
            codewords.insert(hash, codeword);
        }

        let mut digest = Sha256::new();
        digest.update((block_size as u64).to_le_bytes());
        digest.update(alphabet.start.to_le_bytes());
        digest.update(alphabet.size.to_le_bytes());
        for (hash, codeword) in &codewords {
            digest.update(hash.to_le_bytes());
            for token in codeword {
                digest.update(token.to_le_bytes());
            }
        }
        let manifest = TokenDictionaryManifest {
            trace_block_size: block_size,
            token_start: alphabet.start,
            token_alphabet_size: alphabet.size,
            codeword_count: codewords.len(),
            dictionary_digest_sha256: hex::encode(digest.finalize()),
        };

        Ok(Self {
            block_size,
            alphabet,
            codewords,
            manifest,
        })
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
            let codeword = self
                .codewords
                .get(hash)
                .with_context(|| format!("missing codeword for sequence hash {hash}"))?;
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
        debug_assert!(tokens.iter().all(|token| {
            *token >= self.alphabet.start && *token < self.alphabet.start + self.alphabet.size
        }));
        Ok(tokens)
    }
}

fn make_codeword(
    hash: u64,
    nonce: u64,
    block_size: usize,
    alphabet: SafeTokenAlphabet,
) -> Vec<u32> {
    let mut state = hash ^ nonce.rotate_left(23) ^ 0x9e37_79b9_7f4a_7c15;
    (0..block_size)
        .map(|position| {
            state = splitmix64(state ^ (position as u64).wrapping_mul(0xd6e8_feb8_6659_fd93));
            alphabet.start + (state % alphabet.size as u64) as u32
        })
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
    use super::*;
    use crate::trace::TraceRequest;

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

    #[test]
    fn preserves_shared_prefix_and_exact_length() {
        let first = request("a", 6, &[11, 22]);
        let second = request("b", 8, &[11, 33]);
        let dictionary = TokenDictionary::build(
            &[first.clone(), second.clone()],
            SafeTokenAlphabet::new(100, 16).unwrap(),
        )
        .unwrap();
        let first_tokens = dictionary.synthesize(&first).unwrap();
        let second_tokens = dictionary.synthesize(&second).unwrap();

        assert_eq!(first_tokens.len(), 6);
        assert_eq!(second_tokens.len(), 8);
        assert_eq!(&first_tokens[..4], &second_tokens[..4]);
        assert_ne!(&first_tokens[4..], &second_tokens[4..6]);
    }

    #[test]
    fn dictionary_is_independent_of_request_order() {
        let first = request("a", 4, &[11]);
        let second = request("b", 4, &[22]);
        let alphabet = SafeTokenAlphabet::new(100, 16).unwrap();
        let left = TokenDictionary::build(&[first.clone(), second.clone()], alphabet).unwrap();
        let right = TokenDictionary::build(&[second, first], alphabet).unwrap();
        assert_eq!(
            left.manifest.dictionary_digest_sha256,
            right.manifest.dictionary_digest_sha256
        );
    }
}
