"""API endpoints for all API's to connect to"""

# Google
GOOGLE_TOKEN_ENDPOINT = "https://accounts.google.com/o/oauth2/token"

# DCM API endpoints
DCM_REPORTING_AUTHENTICATION_ENDPOINT = ["https://www.googleapis.com/auth/dfareporting"]

# Search Ads 360
GOOGLE_DOUBLE_CLICK_SEARCH_REPORT_ENDPOINT = "https://www.googleapis.com/doubleclicksearch/v2/reports"

# Apple
OAUTH2_API_ENDPOINT = "https://appleid.apple.com/auth/oauth2/token"
APPLE_CAMPAIGN_API_ENDPOINT = "https://api.searchads.apple.com/api/v4/reports/campaigns"

# TikTok
ROOT_URL = "https://business-api.tiktok.com"
TIKTOK_REPORTING_ENDPOINT = f"{ROOT_URL}/open_api/v1.2/reports/integrated/get"