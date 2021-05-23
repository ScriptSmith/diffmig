pub trait Diff<'a> {
    type Difference;

    fn diff(&'a self, comp: &'a Self) -> Option<Vec<Self::Difference>>;
}

/// If a and b are not equal, add the difference to the list of differences
macro_rules! eq_diff {
    ($a:expr, $b:expr, $vec:expr, $enum_variant:expr) => {
        {
            if $a != $b {
                $vec.push($enum_variant($a, $b))
            }
        }
    };

    ($condition:expr, $a:expr, $b:expr, $vec:expr, $enum_variant:expr) => {
        {
            if $condition {
                $vec.push($enum_variant($a, $b))
            }
        }
    };
}

/// If a and b are not the same variant, add the difference to the list of differences
macro_rules! variant_diff {
    ($a:expr, $b:expr, $vec:expr, $enum_variant:expr) => {
        {
            if discriminant($a) != discriminant($b) {
                $vec.push($enum_variant($a, $b))
            }
        }
    };
}

pub(crate) use eq_diff;
pub(crate) use variant_diff;