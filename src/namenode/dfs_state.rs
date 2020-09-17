use crate::block::Block;
use crate::error::{Result, UdfsError};

use std::collections::HashMap;

const VALID_SPECIAL_CHARACTERS: &str = "=-_";

enum IndexNode {
    Directory {
        name: String,
        children: HashMap<String, IndexNode>,
    },
    File {
        name: String,
        blocks: Vec<Block>,
    },
}

impl IndexNode {
    fn add_directory(&mut self, name: &str) -> Result<&mut IndexNode> {
        match self {
            IndexNode::Directory { name: _, children } => {
                let child = children
                    .entry(name.to_owned())
                    .or_insert(IndexNode::Directory {
                        name: name.to_owned(),
                        children: HashMap::new(),
                    });
                Ok(child)
            }

            IndexNode::File { name, blocks: _ } => {
                Err(UdfsError::FSError(format!("'{}' s not a directory", name)))
            }
        }
    }

    fn name(&self) -> &str {
        match self {
            IndexNode::Directory { name, children: _ } => name,
            IndexNode::File { name, blocks: _ } => name,
        }
    }

    fn ls(&self) -> Vec<&str> {
        match self {
            IndexNode::Directory { name: _, children } => {
                children.values().map(|child| child.name()).collect()
            }
            IndexNode::File { name, blocks: _ } => vec![name],
        }
    }
}

fn is_valid_filename(filename: &str) -> bool {
    filename
        .chars()
        .all(|c| char::is_alphanumeric(c) || VALID_SPECIAL_CHARACTERS.contains(c))
}

fn starts_with_root_directory(path: &str) -> Result<&str> {
    path.strip_prefix("/")
        .ok_or_else(|| UdfsError::FSError("Has to start with root directory".to_owned()))
}

pub struct DfsState {
    root: IndexNode,
}

impl DfsState {
    pub fn new() -> Self {
        Self {
            root: IndexNode::Directory {
                name: "".into(),
                children: HashMap::new(),
            },
        }
    }

    pub fn mkdir(&mut self, path: &str) -> Result<()> {
        let path = starts_with_root_directory(path)?;
        let mut node = &mut self.root;
        for part in path.split('/') {
            if part.is_empty() {
                return Err(UdfsError::FSError(
                    "Directory name cannot be empty".to_owned(),
                ));
            }
            if !is_valid_filename(part) {
                return Err(UdfsError::FSError(format!(
                    "'{}' is not a valid directory name.",
                    part
                )));
            }
            node = node.add_directory(part)?;
        }

        Ok(())
    }

    pub fn ls(&self, path: &str) -> Result<Vec<&str>> {
        let path = starts_with_root_directory(path)?;
        if path.is_empty() {
            Ok(self.root.ls())
        } else {
            let node = self.get_node(path)?;
            Ok(node.ls())
        }
    }

    pub fn open_file(&self, path: &str) -> Result<&[Block]> {
        let path = starts_with_root_directory(path)?;
        let node = self.get_node(path)?;

        match node {
            IndexNode::Directory {
                name: _,
                children: _,
            } => Err(UdfsError::FSError(format!(
                "'{}': Is not a file but a directory",
                path
            ))),
            IndexNode::File { name: _, blocks } => Ok(blocks.as_slice()),
        }
    }

    pub fn create_file(&mut self, path: &str, blocks: &[Block]) -> Result<()> {
        let path = starts_with_root_directory(path)?;

        let parts = path.split('/').collect::<Vec<_>>();
        let node = self.get_parent_node_mut(&parts)?;
        let filename = parts[parts.len() - 1];

        match node {
            IndexNode::Directory {
                name: _,
                ref mut children,
            } => match children.get(filename) {
                Some(_) => Err(UdfsError::FSError(format!(
                    "'{}': File or directory already exists",
                    filename
                ))),
                None => {
                    let file = IndexNode::File {
                        name: filename.to_owned(),
                        blocks: blocks.into(),
                    };
                    children.insert(filename.to_owned(), file);
                    Ok(())
                }
            },
            IndexNode::File { name, blocks: _ } => Err(UdfsError::FSError(format!(
                "'{}': Directory expected, but got file",
                name
            ))),
        }
    }

    pub fn check_file_creation(&self, path: &str) -> Result<()> {
        let path = starts_with_root_directory(path)?;
        if path.ends_with('/') {
            return Err(UdfsError::FSError("'/' is not a valid filename".to_owned()));
        }
        if path.is_empty() {
            return Err(UdfsError::FSError("Path and filename required".to_owned()));
        }

        let parts = path.split('/').collect::<Vec<_>>();
        let node = self.get_parent_node(&parts)?;
        let filename = parts[parts.len() - 1];

        match node {
            IndexNode::Directory { name: _, children } => match children.get(filename) {
                Some(_) => Err(UdfsError::FSError(format!(
                    "'{}': File or directory already exists",
                    filename
                ))),
                None => Ok(()),
            },
            IndexNode::File { name, blocks: _ } => Err(UdfsError::FSError(format!(
                "'{}': Directory expected, but got file",
                name
            ))),
        }
    }

    fn get_node(&self, path: &str) -> Result<&IndexNode> {
        let mut node = &self.root;
        for part in path.split('/') {
            match node {
                IndexNode::Directory { name: _, children } => {
                    node = children.get(part).ok_or_else(|| {
                        UdfsError::FSError(format!("'{}': No such file or directory", path))
                    })?;
                }
                IndexNode::File { name: _, blocks: _ } => {
                    return Err(UdfsError::FSError(format!("'{}': Not a directory", path)))
                }
            };
        }
        Ok(node)
    }

    fn get_parent_node(&self, parts: &[&str]) -> Result<&IndexNode> {
        let mut node = &self.root;

        for &part in parts.iter().take(parts.len() - 1) {
            node = match node {
                IndexNode::Directory { name: _, children } => match children.get(part) {
                    Some(node) => node,
                    None => {
                        return Err(UdfsError::FSError(format!("'{}': No such directory", part)))
                    }
                },
                IndexNode::File { name, blocks: _ } => {
                    return Err(UdfsError::FSError(format!(
                        "'{}': Directory expected, but got file",
                        name
                    )))
                }
            };
        }

        Ok(node)
    }

    fn get_parent_node_mut(&mut self, parts: &[&str]) -> Result<&mut IndexNode> {
        let mut node = &mut self.root;

        for &part in parts.iter().take(parts.len() - 1) {
            node = match node {
                IndexNode::Directory { name: _, children } => match children.get_mut(part) {
                    Some(node) => node,
                    None => {
                        return Err(UdfsError::FSError(format!("'{}': No such directory", part)))
                    }
                },
                IndexNode::File { name, blocks: _ } => {
                    return Err(UdfsError::FSError(format!(
                        "'{}': Directory expected, but got file",
                        name
                    )))
                }
            };
        }

        Ok(node)
    }
}

#[cfg(test)]
mod test {

    use super::DfsState;

    #[test]
    fn can_create_directories() {
        let directories = ["/foo", "/foobar", "/foobarbaz", "/foo/bar/baz"];

        let mut state = DfsState::new();
        for dir in &directories {
            assert!(state.mkdir(dir).is_ok());
        }

        let mut root_content = state.ls("/").unwrap();
        root_content.sort_unstable();
        assert_eq!(root_content, vec!["foo", "foobar", "foobarbaz"]);

        let foo_content = state.ls("/foo").unwrap();
        assert_eq!(foo_content, vec!["bar"]);

        let bar_content = state.ls("/foo/bar").unwrap();
        assert_eq!(bar_content, vec!["baz"]);
    }
}
