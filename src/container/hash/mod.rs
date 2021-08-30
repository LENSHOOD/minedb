use crate::container::hash::FindSlotResult::Found;

pub mod hash_table;
pub mod linear_probe_hash_table;

pub enum FindSlotResult<T> {
    NotFound,

    Duplicated,

    Found(T),
}

impl<T> FindSlotResult<T> {
    pub fn not_found(&self) -> bool {
        matches!(self, FindSlotResult::NotFound)
    }

    pub fn duplicated(&self) -> bool {
        matches!(self, FindSlotResult::Duplicated)
    }

    pub fn found(&self) -> bool {
        matches!(self, FindSlotResult::Found(_))
    }

    pub fn unwrap(self) -> T {
        match self {
            Found(val) => val,
            _ => panic!("FindSlotResult cannot get available value"),
        }
    }
}