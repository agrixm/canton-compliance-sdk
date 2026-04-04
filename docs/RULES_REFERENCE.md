# Compliance Rules Reference

## KYC Rules
| Rule | Default | Description |
|------|---------|-------------|
| `kycRequired` | `true` | Block transactions without valid KYC |
| `allowedTiers` | `["retail","institutional"]` | Permitted KYC tier levels |
| Expiry check | Automatic | Expired KYC treated as invalid |

## AML Rules
| Rule | Default | Description |
|------|---------|-------------|
| `amlRequired` | `true` | Block transactions for Blocked risk |
| `velocityLimit` | 100,000 | Max daily transaction amount (USD) |
| Risk levels | Low→Blocked | Low and Medium allowed, High reviewed |

## Travel Rule
Triggered for cross-VASP transfers above $1,000 (FATF threshold).
Sending VASP must include originator + beneficiary info in TravelRuleMessage.
