use std::cmp::Ordering;

pub fn binary_search_by<F>(len: usize, mut f: F) -> Result<usize, usize>
    where F: FnMut(usize) -> Ordering
{
    let mut start = 0;
    let mut end = len;
    while start < end {
        let mid = (start + end) / 2;
        match f(mid) {
            Ordering::Less => start = mid + 1,
            Ordering::Greater => end = mid,
            Ordering::Equal => return Ok(mid),
        }
    }
    Err(start)
}

#[cfg(test)]
mod tests {
    use super::binary_search_by;

    #[test]
    fn test() {
        let arr = vec![1, 2, 3, 5, 8, 13, 21];
        assert_eq!(Ok(0), binary_search_by(arr.len(), |idx| arr[idx].cmp(&1)));
        assert_eq!(Err(0), binary_search_by(arr.len(), |idx| arr[idx].cmp(&0)));
        assert_eq!(Ok(1), binary_search_by(arr.len(), |idx| arr[idx].cmp(&2)));
        assert_eq!(Ok(4), binary_search_by(arr.len(), |idx| arr[idx].cmp(&8)));
        assert_eq!(Err(4), binary_search_by(arr.len(), |idx| arr[idx].cmp(&6)));
        assert_eq!(Ok(6), binary_search_by(arr.len(), |idx| arr[idx].cmp(&21)));
        assert_eq!(Err(7), binary_search_by(arr.len(), |idx| arr[idx].cmp(&22)));
    }
}
