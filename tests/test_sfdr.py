# Copyright (c) 2024 Digital Asset (Canton) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from decimal import Decimal
import os
import sys

# Add the project root to the path to allow importing the sdk module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sdk.formats.sfdr import format_sfdr_disclosure, SFDRValidationError

class TestSFDRFormatter(unittest.TestCase):
    """
    Unit tests for the SFDR (Sustainable Finance Disclosure Regulation) PAI (Principal Adverse Impacts)
    and periodic disclosure formatter. These tests validate the transformation of structured
    Canton contract data into ESMA-compliant disclosure formats for financial products
    classified under Articles 6, 8, and 9 of SFDR.
    """

    def setUp(self):
        """Set up common mock Canton contract payloads for testing."""
        self.article_9_payload = {
            "productId": "ISIN-DE000XYZ1234",
            "productName": "Global Climate Action Equity Fund",
            "legalEntityId": "LEI-54930008U1G7D4N61S73",
            "sfdrArticle": "Article 9",
            "sustainableInvestmentObjective": "This financial product has sustainable investment as its objective, specifically to contribute to climate change mitigation by investing in companies that derive at least 50% of their revenue from renewable energy sources, energy efficiency solutions, and sustainable transportation.",
            "taxonomyAlignedInvestmentPct": Decimal("75.50"),
            "otherSustainableInvestmentPct": Decimal("15.00"),
            "otherInvestmentPct": Decimal("9.50"),
            "principalAdverseImpactsConsidered": True,
            "paiStatementUrl": "https://example.com/pai-statement-2023.pdf",
            "designatedReferenceBenchmark": {
                "name": "MSCI World Climate Paris Aligned Index",
                "isDesignated": True
            }
        }

        self.article_8_payload = {
            "productId": "ISIN-FR000ABC5678",
            "productName": "European Socially Responsible Fund",
            "legalEntityId": "LEI-54930008U1G7D4N61S73",
            "sfdrArticle": "Article 8",
            "promotesEsCharacteristics": "This financial product promotes environmental and social characteristics by investing in companies with strong ESG ratings based on proprietary analysis and by applying specific exclusions, such as for companies involved in controversial weapons, tobacco, and thermal coal.",
            "sustainableInvestmentPct": Decimal("20.00"),
            "taxonomyAlignedInvestmentPct": Decimal("5.00"),
            "otherInvestmentPct": Decimal("80.00"),
            "principalAdverseImpactsConsidered": True,
            "paiStatementUrl": "https://example.com/pai-statement-2023.pdf",
            "designatedReferenceBenchmark": {
                "name": "Dow Jones Sustainability Europe Index",
                "isDesignated": True
            }
        }

        self.article_6_payload = {
            "productId": "ISIN-US000DEF9012",
            "productName": "Global Diversified Holdings",
            "legalEntityId": "LEI-54930008U1G7D4N61S73",
            "sfdrArticle": "Article 6",
            "principalAdverseImpactsConsidered": False,
            "paiNonConsiderationExplanation": "The investment strategy of this product does not consider the principal adverse impacts on sustainability factors as the underlying data is not yet sufficiently available or reliable to perform a thorough assessment."
        }

    def test_format_article_9_product_successful(self):
        """
        Tests formatting for a compliant Article 9 product with a sustainable investment objective.
        """
        report = format_sfdr_disclosure(self.article_9_payload)

        # Validate summary section
        self.assertEqual(report['summary']['productIdentifier'], "ISIN-DE000XYZ1234")
        self.assertEqual(report['summary']['productName'], "Global Climate Action Equity Fund")
        self.assertEqual(report['summary']['sfdrClassification'], "Article 9")

        # Validate sustainability objective
        self.assertTrue(report['sustainabilityObjective']['hasSustainableObjective'])
        self.assertIn("climate change mitigation", report['sustainabilityObjective']['objectiveDescription'])
        self.assertIsNone(report.get('environmentalSocialCharacteristics'))

        # Validate asset allocation and proportions
        self.assertEqual(report['assetAllocation']['taxonomyAlignedPct'], 75.50)
        self.assertEqual(report['assetAllocation']['otherSustainablePct'], 15.00)
        self.assertEqual(report['assetAllocation']['totalSustainablePct'], 90.50)  # 75.5 + 15.0
        self.assertEqual(report['assetAllocation']['otherPct'], 9.50)

        # Validate PAI section
        self.assertTrue(report['principalAdverseImpacts']['isConsidered'])
        self.assertEqual(report['principalAdverseImpacts']['statementUrl'], "https://example.com/pai-statement-2023.pdf")

        # Validate benchmark
        self.assertTrue(report['referenceBenchmark']['isDesignated'])
        self.assertEqual(report['referenceBenchmark']['benchmarkName'], "MSCI World Climate Paris Aligned Index")

    def test_format_article_8_product_successful(self):
        """
        Tests formatting for a compliant Article 8 product promoting E/S characteristics.
        """
        report = format_sfdr_disclosure(self.article_8_payload)

        # Validate summary section
        self.assertEqual(report['summary']['productIdentifier'], "ISIN-FR000ABC5678")
        self.assertEqual(report['summary']['sfdrClassification'], "Article 8")

        # Validate E/S characteristics section
        self.assertFalse(report['sustainabilityObjective']['hasSustainableObjective'])
        self.assertIsNone(report['sustainabilityObjective']['objectiveDescription'])
        self.assertTrue(report['environmentalSocialCharacteristics']['promotesEs'])
        self.assertIn("strong ESG ratings", report['environmentalSocialCharacteristics']['characteristicsDescription'])

        # Validate asset allocation. Note: for Art 8, sustainable investments are a subset.
        self.assertEqual(report['assetAllocation']['sustainableInvestmentPct'], 20.00)
        self.assertEqual(report['assetAllocation']['taxonomyAlignedPct'], 5.00) # subset of the 20%
        self.assertEqual(report['assetAllocation']['otherPct'], 80.00)

        # Validate PAI section
        self.assertTrue(report['principalAdverseImpacts']['isConsidered'])
        self.assertEqual(report['principalAdverseImpacts']['statementUrl'], "https://example.com/pai-statement-2023.pdf")

    def test_format_article_6_product_successful(self):
        """
        Tests formatting for a standard Article 6 product with no sustainability focus.
        """
        report = format_sfdr_disclosure(self.article_6_payload)

        # Validate summary section
        self.assertEqual(report['summary']['productIdentifier'], "ISIN-US000DEF9012")
        self.assertEqual(report['summary']['sfdrClassification'], "Article 6")
        
        # Validate that sustainability sections are explicitly negative/empty
        self.assertFalse(report['sustainabilityObjective']['hasSustainableObjective'])
        self.assertFalse(report['environmentalSocialCharacteristics']['promotesEs'])
        
        # Validate PAI non-consideration statement
        self.assertFalse(report['principalAdverseImpacts']['isConsidered'])
        self.assertIn("data is not yet sufficiently available", report['principalAdverseImpacts']['nonConsiderationReason'])
        self.assertIsNone(report['principalAdverseImpacts'].get('statementUrl'))
        
        # Validate empty allocation and benchmark sections
        self.assertEqual(report['assetAllocation']['taxonomyAlignedPct'], 0.0)
        self.assertEqual(report['assetAllocation']['sustainableInvestmentPct'], 0.0)
        self.assertFalse(report['referenceBenchmark']['isDesignated'])

    def test_missing_required_field_raises_error(self):
        """
        Tests that a missing required field for a specific article type raises SFDRValidationError.
        """
        # Article 9 products require a sustainableInvestmentObjective
        invalid_payload = self.article_9_payload.copy()
        del invalid_payload['sustainableInvestmentObjective']

        with self.assertRaises(SFDRValidationError) as context:
            format_sfdr_disclosure(invalid_payload)

        self.assertIn("'sustainableInvestmentObjective' is a required field for Article 9 products", str(context.exception))

    def test_inconsistent_percentages_raise_error(self):
        """
        Tests that asset allocation percentages for an Article 9 fund that do not sum to 100 raise an error.
        """
        invalid_payload = self.article_9_payload.copy()
        # 75.5 + 15.0 + 10.0 = 100.5, which is incorrect
        invalid_payload['otherInvestmentPct'] = Decimal("10.00")

        with self.assertRaises(SFDRValidationError) as context:
            format_sfdr_disclosure(invalid_payload)

        self.assertIn("Article 9 asset allocation percentages must sum to 100", str(context.exception))
        self.assertIn("current sum is 100.5", str(context.exception))

    def test_article_8_inconsistent_percentages_raise_error(self):
        """
        Tests that asset allocation percentages for an Article 8 fund that do not sum to 100 raise an error.
        """
        invalid_payload = self.article_8_payload.copy()
        # 20.0 (sustainable) + 70.0 (other) = 90.0, incorrect
        invalid_payload['otherInvestmentPct'] = Decimal("70.00")

        with self.assertRaises(SFDRValidationError) as context:
            format_sfdr_disclosure(invalid_payload)

        self.assertIn("Article 8 asset allocation percentages must sum to 100", str(context.exception))
        self.assertIn("current sum is 90.0", str(context.exception))
        
    def test_taxonomy_greater_than_sustainable_raises_error(self):
        """
        Tests that taxonomy-aligned percentage cannot exceed total sustainable investment percentage for an Article 8 product.
        """
        invalid_payload = self.article_8_payload.copy()
        invalid_payload['taxonomyAlignedInvestmentPct'] = Decimal("25.00") # > sustainableInvestmentPct of 20.00
        
        with self.assertRaises(SFDRValidationError) as context:
            format_sfdr_disclosure(invalid_payload)

        self.assertIn("Taxonomy-aligned investments (25.00%) cannot exceed total sustainable investments (20.00%)", str(context.exception))


    def test_invalid_sfdr_article_value_raises_error(self):
        """
        Tests that an unsupported value for the 'sfdrArticle' field raises a validation error.
        """
        invalid_payload = self.article_6_payload.copy()
        invalid_payload['sfdrArticle'] = "Article 10"  # This is not a valid classification

        with self.assertRaises(SFDRValidationError) as context:
            format_sfdr_disclosure(invalid_payload)

        self.assertIn("Invalid value for 'sfdrArticle': 'Article 10'. Must be one of", str(context.exception))

if __name__ == '__main__':
    unittest.main(verbosity=2)
