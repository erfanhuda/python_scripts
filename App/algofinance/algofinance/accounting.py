import re

TEST_ACCOUNTING = """
2023-12-31 spending
    assets:inventory:lumbers IDR 200,000,000
    equity:checking balance
"""

TEST_TRANSACTION = """
2023-12-31 spending
    assets:inventory:lumbers IDR 200,000,000
    equity:checking balance
"""

def _find_the_date_record():
    pass

def _find_the_record_accounts():
    pass

print(" ".join(TEST_ACCOUNTING.split()))