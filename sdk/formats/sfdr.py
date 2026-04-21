# sdk/formats/sfdr.py

"""
SFDR (Sustainable Finance Disclosure Regulation) Data Models and Transformation Logic.

This module provides Pydantic models for structuring SFDR data, specifically focusing on
Article 8 and Article 9 product disclosures, including Principal Adverse Impact (PAI)
indicators and the "Do No Significant Harm" (DNSH) principle. It also includes a
transformer function to convert raw Daml contract data into these structured formats.

The models are based on the Regulatory Technical Standards (RTS) for SFDR.
"""

from datetime import date
from decimal import Decimal
from typing import Optional, Literal, Dict, Any, Union
from uuid import uuid4

from pydantic import BaseModel, Field, validator


class PrincipalAdverseImpacts(BaseModel):
    """
    Models the Principal Adverse Impact (PAI) indicators as defined in the SFDR RTS Annex I.
    These are used to assess the negative effects of investment decisions on sustainability factors.
    """
    # Table 1: Mandatory indicators for all investments
    # Climate and other environment-related indicators
    ghg_emissions_scope1: Optional[Decimal] = Field(None, description="Indicator 1.1: Scope 1 GHG emissions (tCO2e / EUR million invested)")
    ghg_emissions_scope2: Optional[Decimal] = Field(None, description="Indicator 1.2: Scope 2 GHG emissions (tCO2e / EUR million invested)")
    ghg_emissions_scope3: Optional[Decimal] = Field(None, description="Indicator 1.3: Scope 3 GHG emissions (tCO2e / EUR million invested)")
    total_ghg_emissions: Optional[Decimal] = Field(None, description="Indicator 1.4: Total GHG emissions (Scope 1+2+3)")
    carbon_footprint: Optional[Decimal] = Field(None, description="Indicator 2: Carbon footprint (tCO2e / EUR million invested)")
    ghg_intensity_investee: Optional[Decimal] = Field(None, description="Indicator 3: GHG intensity of investee companies")
    exposure_fossil_fuel_sector: Optional[Decimal] = Field(None, description="Indicator 4: Exposure to companies active in the fossil fuel sector (%)")
    energy_consumption_intensity: Optional[Decimal] = Field(None, description="Indicator 5: Share of non-renewable energy consumption and production")

    # Social and employee, respect for human rights, anti-corruption and anti-bribery matters
    violations_un_global_compact: Optional[Decimal] = Field(None, description="Indicator 10: Violations of UN Global Compact principles and OECD Guidelines for Multinational Enterprises (%)")
    lack_of_grievance_mechanisms: Optional[Decimal] = Field(None, description="Indicator 11: Lack of processes and compliance mechanisms to monitor compliance with UNGC principles (%)")
    unadjusted_gender_pay_gap: Optional[Decimal] = Field(None, description="Indicator 12: Unadjusted gender pay gap (%)")
    board_gender_diversity: Optional[Decimal] = Field(None, description="Indicator 13: Board gender diversity (%)")
    exposure_controversial_weapons: Optional[Decimal] = Field(None, description="Indicator 14: Exposure to controversial weapons (anti-personnel mines, cluster munitions, etc.) (%)")

    class Config:
        orm_mode = True
        anystr_strip_whitespace = True


class DNSHAssessment(BaseModel):
    """
    Models the "Do No Significant Harm" (DNSH) assessment against the six environmental
    objectives of the EU Taxonomy Regulation.
    """
    climate_change_mitigation: bool = Field(..., description="Assessment of significant harm to climate change mitigation.")
    climate_change_adaptation: bool = Field(..., description="Assessment of significant harm to climate change adaptation.")
    sustainable_water_use: bool = Field(..., description="Assessment of significant harm to sustainable use and protection of water and marine resources.")
    circular_economy_transition: bool = Field(..., description="Assessment of significant harm to the transition to a circular economy.")
    pollution_prevention: bool = Field(..., description="Assessment of significant harm to pollution prevention and control.")
    biodiversity_protection: bool = Field(..., description="Assessment of significant harm to the protection and restoration of biodiversity and ecosystems.")
    assessment_summary: str = Field(..., description="Narrative explaining how the PAI indicators were used to determine that the investment does no significant harm.")

    class Config:
        orm_mode = True
        anystr_strip_whitespace = True


class SFDRReport(BaseModel):
    """
    Represents a structured SFDR report for a financial product, generated from
    on-ledger data.
    """
    report_id: str = Field(default_factory=lambda: str(uuid4()), description="Unique identifier for this report instance.")
    reporting_entity_party: str = Field(..., description="Daml Party ID of the reporting entity (e.g., asset manager).")
    product_isin: str = Field(..., description="ISIN of the financial product.")
    reference_date: date = Field(..., description="The 'as-of' date for the data in the report.")
    article_classification: Literal["Article 6", "Article 8", "Article 9"] = Field(..., description="SFDR product classification.")
    
    # Optional sections, primarily applicable to Article 8 and 9 products
    principal_adverse_impacts: Optional[PrincipalAdverseImpacts] = Field(None, description="Principal Adverse Impact (PAI) indicator values.")
    dnsh_assessment: Optional[DNSHAssessment] = Field(None, description="Do No Significant Harm (DNSH) assessment results.")
    
    sustainable_investment_objective_summary: Optional[str] = Field(None, description="Summary of how the investments of the financial product attain the sustainable investment objective (for Article 9 products).")
    taxonomy_alignment_percentage: Optional[Decimal] = Field(None, description="Percentage of the investment aligned with the EU Taxonomy.")

    @validator('taxonomy_alignment_percentage')
    def check_percentage(cls, v):
        if v is not None and not (0 <= v <= 100):
            raise ValueError('taxonomy_alignment_percentage must be between 0 and 100')
        return v

    class Config:
        orm_mode = True


def _to_decimal(value: Any) -> Optional[Decimal]:
    """Safely convert a value to a Decimal, returning None if conversion fails."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None

def transform_to_sfdr(
    daml_contract_payload: Dict[str, Any],
    reporting_party: str,
    report_date: date
) -> SFDRReport:
    """
    Transforms a raw Daml contract payload representing a financial product
    into a structured SFDR report.

    This function expects the payload to contain a nested 'esgData' field with the
    relevant SFDR information. It handles missing sub-fields gracefully.

    Args:
        daml_contract_payload: The 'payload' dictionary from a Daml contract fetched via the JSON API.
        reporting_party: The Daml Party ID of the entity generating the report.
        report_date: The reference date for the report.

    Returns:
        An instance of the SFDRReport Pydantic model.

    Raises:
        KeyError: If mandatory fields like 'isin' or 'esgData' are missing.
        ValueError: If the SFDR article classification is invalid.
    """
    isin = daml_contract_payload.get("isin")
    if not isin:
        raise KeyError("Daml payload must contain an 'isin' field.")

    esg_data = daml_contract_payload.get("esgData")
    if not esg_data:
        raise KeyError("Daml payload must contain an 'esgData' field for SFDR reporting.")

    article = esg_data.get("sfdrArticleClassification")
    if article not in ["Article 6", "Article 8", "Article 9"]:
        raise ValueError(f"Invalid SFDR classification: {article}")

    pai_data = esg_data.get("principalAdverseImpacts", {})
    dnsh_data = esg_data.get("dnshAssessment", {})

    pai_report = None
    # PAI data is only relevant for Article 8 and 9 products
    if article in ["Article 8", "Article 9"] and pai_data:
        pai_report = PrincipalAdverseImpacts(
            ghg_emissions_scope1=_to_decimal(pai_data.get("ghgEmissionsScope1")),
            ghg_emissions_scope2=_to_decimal(pai_data.get("ghgEmissionsScope2")),
            ghg_emissions_scope3=_to_decimal(pai_data.get("ghgEmissionsScope3")),
            total_ghg_emissions=_to_decimal(pai_data.get("totalGhgEmissions")),
            carbon_footprint=_to_decimal(pai_data.get("carbonFootprint")),
            ghg_intensity_investee=_to_decimal(pai_data.get("ghgIntensityInvestee")),
            exposure_fossil_fuel_sector=_to_decimal(pai_data.get("exposureFossilFuelSector")),
            energy_consumption_intensity=_to_decimal(pai_data.get("energyConsumptionIntensity")),
            violations_un_global_compact=_to_decimal(pai_data.get("violationsUnGlobalCompact")),
            lack_of_grievance_mechanisms=_to_decimal(pai_data.get("lackOfGrievanceMechanisms")),
            unadjusted_gender_pay_gap=_to_decimal(pai_data.get("unadjustedGenderPayGap")),
            board_gender_diversity=_to_decimal(pai_data.get("boardGenderDiversity")),
            exposure_controversial_weapons=_to_decimal(pai_data.get("exposureControversialWeapons")),
        )

    dnsh_report = None
    # DNSH assessment is a core part of Article 8 & 9 products with sustainable characteristics/objectives
    if article in ["Article 8", "Article 9"] and dnsh_data:
        dnsh_report = DNSHAssessment(
            climate_change_mitigation=dnsh_data.get("climateChangeMitigation", False),
            climate_change_adaptation=dnsh_data.get("climateChangeAdaptation", False),
            sustainable_water_use=dnsh_data.get("sustainableWaterUse", False),
            circular_economy_transition=dnsh_data.get("circularEconomyTransition", False),
            pollution_prevention=dnsh_data.get("pollutionPrevention", False),
            biodiversity_protection=dnsh_data.get("biodiversityProtection", False),
            assessment_summary=dnsh_data.get("assessmentSummary", "No summary provided."),
        )

    report = SFDRReport(
        reporting_entity_party=reporting_party,
        product_isin=isin,
        reference_date=report_date,
        article_classification=article,
        principal_adverse_impacts=pai_report,
        dnsh_assessment=dnsh_report,
        sustainable_investment_objective_summary=esg_data.get("sustainableInvestmentObjectiveSummary"),
        taxonomy_alignment_percentage=_to_decimal(esg_data.get("taxonomyAlignmentPercentage"))
    )

    return report