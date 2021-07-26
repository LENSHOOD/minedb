use fasthash::XXHasher;
use std::hash::{Hash, Hasher};
use crate::common::KeyType;

pub trait HashKeyType: KeyType + Hash {}
impl<T: HashKeyType> KeyType for T {}

pub fn hash<K: HashKeyType>(key: &K) -> u64 {
    let mut hasher: XXHasher = Default::default();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fasthash::xx::hash64;

    #[derive(Hash)]
    struct TestHashKey {
        i: u8,
    }
    impl HashKeyType for TestHashKey {}

    #[test]
    fn should_cal_hash_for_hash_key_type() {
        // given
        let key = TestHashKey {
            i: 23,
        };

        // when
        let actual = hash(&key);

        // then
        let mut v = vec![];
        v.push(key.i);
        assert_eq!(hash64(v), actual);
    }
}