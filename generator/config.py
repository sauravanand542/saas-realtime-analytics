"""Simulated commercial catalog.

These prices are inputs to the generator, not measurements. The warehouse
trusts the MRR recorded on each subscription event rather than recomputing it.
"""

PLAN_ORDER = ["trial", "starter", "growth", "enterprise"]

PLAN_MRR = {
    "trial": 0,
    "starter": 4900,
    "growth": 19900,
    "enterprise": 99900,
}

SEAT_RANGES = {
    "trial": (2, 8),
    "starter": (5, 25),
    "growth": (15, 80),
    "enterprise": (40, 400),
}

INDUSTRIES = [
    "software",
    "fintech",
    "healthcare",
    "education",
    "logistics",
    "media",
    "manufacturing",
    "professional_services",
]

EMPLOYEE_BANDS = ["1-10", "11-50", "51-200", "201-1000", "1000+"]

COUNTRIES = ["US", "GB", "DE", "IN", "CA", "AU", "FR", "NL"]

IDPS = ["okta", "azure_ad", "google_workspace", "onelogin"]

ROLES = ["owner", "admin", "member"]

LOOKBACK_DAYS = 2
