"""API endpoints"""

# NOTE: The motivation for keeping all endpoints in one place and not in respective
# connector module's is for organization and reusability of various tokens
# (i.e. the Google oauth token refresh endpoint as this gets reused in a lot of 
# places)

# Google token refresh
GOOGLE_TOKEN_ENDPOINT = "https://accounts.google.com/o/oauth2/token"

# DCM API endpoints
DCM_REPORTING_AUTHENTICATION_ENDPOINT = ["https://www.googleapis.com/auth/dfareporting"]

# Search Ads 360
GOOGLE_DOUBLE_CLICK_SEARCH_REPORT_ENDPOINT = "https://www.googleapis.com/doubleclicksearch/v2/reports"
SA360_PARTNER_UPLOAD_SFTP_HOST = "partnerupload.google.com"
SA360_PARTNER_UPLOAD_SFTP_PORT = 19321

# Google Drive
GOOGLE_DRIVE_AUTHENTICATION_ENDPOINT = "https://www.googleapis.com/auth/drive"

# Apple
OAUTH2_API_ENDPOINT = "https://appleid.apple.com/auth/oauth2/token"
APPLE_CAMPAIGN_API_ENDPOINT = "https://api.searchads.apple.com/api/v4/reports/campaigns"

# TikTok
ROOT_URL = "https://business-api.tiktok.com"
TIKTOK_REPORTING_ENDPOINT = f"{ROOT_URL}/open_api/v1.2/reports/integrated/get"

# LinkedIn
AD_CAMPAIGNS_ENDPOINT = "https://api.linkedin.com/v2/adCampaignsV2"
AD_ANALYTICS_ENDPOINT = "https://api.linkedin.com/v2/adAnalyticsV2"