# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Basel III module for Standardised Approach for Counterparty Credit Risk (SA-CCR) calculations and reporting.
- Pluggable exporter architecture allowing users to define custom reporting formats.
- `SFDR` data module to capture Principal Adverse Impact (PAI) indicators on financial instruments.
- Support for Commodity Swaps in the core data model and FpML exporter.

### Changed
- **BREAKING**: Upgraded Daml SDK to `3.1.0`. `daml.yaml` and CI scripts have been updated accordingly.
- Optimized ledger queries in the extraction scripts to use `fetchbyKey` where possible, improving performance on ledgers with large contract volumes.
- Refactored MiFID II reporting logic to better handle complex trade amendments and corrections.

### Fixed
- Fixed an issue where null values in optional trade fields were not being correctly serialized in JSON output.

## [0.2.0] - 2024-04-15

### Added
- European Market Infrastructure Regulation (EMIR) reporting module.
  - Includes templates for tracking Unique Trade Identifier (UTI) generation and sharing.
  - Script to generate EMIR-compliant trade state reports for submission to trade repositories.
- ISO 20022 `auth.092` (TradeStateReport) XML exporter.
- Support for Equity Options in the core `Trade` data model.

### Changed
- Refactored the base `Trade` template to separate economic terms from lifecycle state, improving clarity and composability.
- Improved error logging in all data extraction scripts, providing more context on failed transformations.

### Fixed
- Corrected UTC timestamp formatting in MiFID II reports to comply with regulatory technical standards (RTS).
- Ensured currency codes are consistently represented in ISO 4217 format across all exporters.

## [0.1.0] - 2024-01-20

### Added
- Initial release of the Canton Compliance SDK.
- Core Daml data model for OTC derivatives trade lifecycle events, including `Trade`, `Novation`, and `Termination` templates.
- MiFID II post-trade transparency reporting module (RTS 1 & RTS 2 for systematic internalisers).
- Initial FpML 5.10 exporter for simple Interest Rate Swaps (IRS).
- Daml Script `Main.daml` for setting up reference data and running extraction examples.
- Basic project structure with `daml.yaml`, `.gitignore`, and `README.md`.
- GitHub Actions workflow for continuous integration (build and test Daml templates).