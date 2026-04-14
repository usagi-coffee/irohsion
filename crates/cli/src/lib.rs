use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
};

use anyhow::Result;
use iroh::{RelayMode, RelayUrl, SecretKey};
use transport::{InterfaceBinding, resolve_interface_ipv4};

#[derive(Clone, Debug)]
pub enum SecretArg {
    Generate,
    Provided(SecretKey),
}

#[derive(Clone, Debug)]
pub struct InterfaceSpec {
    pub name: String,
    pub secret: Option<SecretKey>,
}

pub struct InterfaceConfig {
    pub binding: InterfaceBinding,
    pub endpoint_id: String,
    pub secret_key: SecretKey,
}

impl FromStr for InterfaceSpec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((name, secret)) = s.rsplit_once(':') {
            if name.is_empty() {
                return Err("interface name cannot be empty".to_string());
            }

            let secret = SecretKey::from_str(secret).map_err(|_| {
                "invalid interface secret; expected iroh secret key hex".to_string()
            })?;
            return Ok(Self {
                name: name.to_string(),
                secret: Some(secret),
            });
        }

        if s.is_empty() {
            return Err("interface name cannot be empty".to_string());
        }

        Ok(Self {
            name: s.to_string(),
            secret: None,
        })
    }
}

impl Default for SecretArg {
    fn default() -> Self {
        Self::Generate
    }
}

impl FromStr for SecretArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(Self::Generate);
        }

        let secret = SecretKey::from_str(s)
            .map_err(|_| "invalid --secret; expected iroh secret key hex".to_string())?;
        Ok(Self::Provided(secret))
    }
}

impl SecretArg {
    pub fn provided(&self) -> Option<&SecretKey> {
        match self {
            Self::Generate => None,
            Self::Provided(secret) => Some(secret),
        }
    }

    pub fn resolve(&self) -> SecretKey {
        self.provided()
            .cloned()
            .unwrap_or_else(|| SecretKey::generate(&mut rand::rng()))
    }
}

pub fn local_udp_dest(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}

pub fn relay_mode(relays: Vec<RelayUrl>) -> RelayMode {
    if relays.is_empty() {
        RelayMode::Default
    } else {
        RelayMode::custom(relays)
    }
}

pub fn parse_interface_configs(specs: &[InterfaceSpec]) -> Result<Vec<InterfaceConfig>> {
    specs.iter().map(build_interface_config).collect()
}

fn build_interface_config(spec: &InterfaceSpec) -> Result<InterfaceConfig> {
    let secret_key = spec
        .secret
        .clone()
        .unwrap_or_else(|| SecretKey::generate(&mut rand::rng()));
    let binding = resolve_interface_ipv4(&spec.name)?;
    let endpoint_id = secret_key.public().to_string();
    Ok(InterfaceConfig {
        binding,
        endpoint_id,
        secret_key,
    })
}
