use crate::rti::{
    ResponseLoginInfo, request_account_list, request_account_rms_info, request_bracket_order,
    request_cancel_all_orders, response_login_info::UserType,
};

/// What a login grants, used to scope requests by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoginScope {
    pub(crate) fcm_id: Option<String>,
    pub(crate) ib_id: Option<String>,
    pub(crate) user_type: UserType,
}

impl LoginScope {
    /// `None` for a user type absent or out of range.
    pub(crate) fn from_login_info(info: &ResponseLoginInfo) -> Option<Self> {
        Some(LoginScope {
            fcm_id: info.fcm_id.clone(),
            ib_id: info.ib_id.clone(),
            user_type: UserType::try_from(info.user_type?).ok()?,
        })
    }
}

/// The user type `scope` grants, or `Trader` without one.
pub(crate) fn user_type_or_trader(scope: Option<&LoginScope>) -> UserType {
    scope.map_or(UserType::Trader, |s| s.user_type)
}

// Each request proto declares its own `UserType`. Mapped by name, not by cast:
// the numbers agree today, but a cast would keep sending the old one if a proto
// were renumbered.

impl From<UserType> for request_bracket_order::UserType {
    fn from(user_type: UserType) -> Self {
        match user_type {
            UserType::Admin => Self::Admin,
            UserType::Fcm => Self::Fcm,
            UserType::Ib => Self::Ib,
            UserType::Trader => Self::Trader,
        }
    }
}

impl From<UserType> for request_cancel_all_orders::UserType {
    fn from(user_type: UserType) -> Self {
        match user_type {
            UserType::Admin => Self::Admin,
            UserType::Fcm => Self::Fcm,
            UserType::Ib => Self::Ib,
            UserType::Trader => Self::Trader,
        }
    }
}

/// `None` for Admin, which template 302 lacks: the optional field is left off
/// rather than sending Trader.
pub(crate) fn account_list_user_type(
    user_type: UserType,
) -> Option<request_account_list::UserType> {
    match user_type {
        UserType::Admin => None,
        UserType::Fcm => Some(request_account_list::UserType::Fcm),
        UserType::Ib => Some(request_account_list::UserType::Ib),
        UserType::Trader => Some(request_account_list::UserType::Trader),
    }
}

/// `None` for Admin, which template 304 lacks.
pub(crate) fn account_rms_info_user_type(
    user_type: UserType,
) -> Option<request_account_rms_info::UserType> {
    match user_type {
        UserType::Admin => None,
        UserType::Fcm => Some(request_account_rms_info::UserType::Fcm),
        UserType::Ib => Some(request_account_rms_info::UserType::Ib),
        UserType::Trader => Some(request_account_rms_info::UserType::Trader),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A login granting `user_type`.
    fn test_login_info(user_type: UserType) -> ResponseLoginInfo {
        ResponseLoginInfo {
            template_id: 301,
            fcm_id: Some("FCM_LOGIN".to_string()),
            ib_id: Some("IB_LOGIN".to_string()),
            user_type: Some(user_type.into()),
            ..ResponseLoginInfo::default()
        }
    }

    #[test]
    fn login_scope_keeps_every_user_type_and_the_ids() {
        for user_type in [
            UserType::Admin,
            UserType::Fcm,
            UserType::Ib,
            UserType::Trader,
        ] {
            let scope = LoginScope::from_login_info(&test_login_info(user_type))
                .unwrap_or_else(|| panic!("{user_type:?} is a known user type"));

            assert_eq!(scope.user_type, user_type);
            assert_eq!(scope.fcm_id.as_deref(), Some("FCM_LOGIN"));
            assert_eq!(scope.ib_id.as_deref(), Some("IB_LOGIN"));
        }
    }

    #[test]
    fn login_scope_rejects_unknown_or_missing_user_types() {
        for info in [
            ResponseLoginInfo {
                user_type: Some(99),
                ..test_login_info(UserType::Ib)
            },
            ResponseLoginInfo {
                user_type: None,
                ..test_login_info(UserType::Ib)
            },
        ] {
            assert!(
                LoginScope::from_login_info(&info).is_none(),
                "{:?} must not produce a scope",
                info.user_type
            );
        }
    }
}
