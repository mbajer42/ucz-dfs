use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub enum UdfsError {
    IOError(String),
    RPCError(String),
    AddressParseError(String),
    ConfigError(String),
    FSError(String),
    ArgMissingError(String),
    WaitingForReplication(String),
    ProtoEncodeError(prost::EncodeError),
    ProtoDecodeError(prost::DecodeError),
}

impl Display for UdfsError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for UdfsError {}

impl From<std::io::Error> for UdfsError {
    fn from(error: std::io::Error) -> Self {
        UdfsError::IOError(error.to_string())
    }
}

impl From<tonic::transport::Error> for UdfsError {
    fn from(error: tonic::transport::Error) -> Self {
        UdfsError::RPCError(error.to_string())
    }
}

impl From<std::net::AddrParseError> for UdfsError {
    fn from(error: std::net::AddrParseError) -> Self {
        UdfsError::AddressParseError(error.to_string())
    }
}

impl From<serde_json::Error> for UdfsError {
    fn from(error: serde_json::Error) -> Self {
        UdfsError::ConfigError(error.to_string())
    }
}

impl From<toml::de::Error> for UdfsError {
    fn from(error: toml::de::Error) -> Self {
        UdfsError::ConfigError(error.to_string())
    }
}

impl From<tonic::Status> for UdfsError {
    fn from(error: tonic::Status) -> Self {
        UdfsError::RPCError(error.to_string())
    }
}

impl From<prost::EncodeError> for UdfsError {
    fn from(error: prost::EncodeError) -> Self {
        UdfsError::ProtoEncodeError(error)
    }
}

impl From<prost::DecodeError> for UdfsError {
    fn from(error: prost::DecodeError) -> Self {
        UdfsError::ProtoDecodeError(error)
    }
}

pub type Result<T> = std::result::Result<T, UdfsError>;
