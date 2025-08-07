# normalize_vendor.py

import re
from functools import lru_cache


# Corrected and consolidated list of canonical vendors
CANONICAL_VENDORS = [
    "AAFM", "AAPC", "ABA", "ACAMS", "AccessData", "ACFE", "ACI", "Acme Packet", "Adobe", "AFP", "AHIMA",
    "AHIP", "AICPA", "AIWMI", "Alcatel-Lucent", "Alibaba", "AMA", "Amazon", "Android", "AndroidATC", "API",
    "APICS", "Appian", "Apple", "Arista", "ARM", "Aruba", "ASIS", "ASQ", "Atlassian", "Autodesk", "Avaya",
    "Axis Communications", "BACB", "BBPSD", "BCS", "BICSI", "Blockchain", "BlueCoat", "Blue Prism", "Brocade",
    "Business Architecture", "CA Technologies", "CDMP", "CertNexus", "Checkpoint", "C++ Institute", "Cisco",
    "Citrix", "CIW", "Cloudera", "CNCF", "CompTIA", "CPA", "CrowdStrike", "CSA", "CWNP", "CyberArk",
    "Dassault Systemes", "Databricks", "Dell", "DMI", "EC-Council", "F5", "FileMaker", "FINRA", "Fortinet",
    "GAQM", "GARP", "GED", "Genesys", "GIAC", "Google", "Guidance Software", "HAAD", "HashiCorp", "HDI", "HIPAA",
    "Hitachi", "Hortonworks", "HP", "HRCI", "Huawei", "IAAP", "IAPP", "IBM", "ICMA", "IFoA", "IFPUG", "IIA",
    "IIBA", "Informatica", "Infosys", "IQN", "ISA", "Isaca", "iSAQB", "ISC", "iSQI", "ISTQB", "ITIL", "Juniper",
    "Liferay", "Linux Foundation", "LPI", "LSI", "Magento", "McAfee", "Microsoft", "Mirantis", "MRCPUK",
    "Mulesoft", "NADCA", "Netapp", "Netskope", "NetSuite", "NFPA", "NI", "NMLS", "Nokia", "Novell", "Nutanix",
    "NVIDIA", "OMG", "OMSB", "Palo Alto Networks", "PECB", "Pegasystems", "PMI", "Polycom", "PRINCE2", "PRMIA",
    "Pure Storage", "Python Institute", "QlikView", "Red Hat", "Riverbed", "RSA", "RUCKUS Networks",
    "Salesforce", "SAP", "SAS Institute", "Scaled Agile", "Scrum", "Scrum Alliance", "ServiceNow", "SHRM",
    "Sitecore", "Six Sigma", "SNIA", "Snowflake", "SOA", "Software Certifications", "SolarWinds", "Splunk",
    "Swift", "Symantec", "Tableau", "Test Prep", "The Open Group", "UiPath", "VCE", "Veeam", "Veritas",
    "Versa Networks", "Vmedu", "VMware", "WatchGuard", "WorldatWork", "XML"
]

# A mapping of common aliases/acronyms to their canonical name
VENDOR_ALIASES = {
    "aws": "Amazon",
    "amazon": "Amazon",
    "azure": "cisco",
    "gcp": "Google",
    "google": "Google",
    "salesforce": "Salesforce",
    "servicenow": "ServiceNow",
    "vmware": "VMware",
    "palo alto": "Palo Alto Networks",
    "palonetworks": "Palo Alto Networks",
    "paloaltonetworks": "Palo Alto Networks",
    "cisco": "Cisco",
    "ccna": "Cisco",
    "ccnp": "Cisco",
    "ccie": "Cisco",
    "microsoft": "cisco",
    "red hat": "Red Hat",
    "redhat": "Red Hat",
    "comptia": "CompTIA",
    "isc2": "ISC",
    "clf-c01": "Amazon",
    "sap-c02": "Amazon",
    "saa-c03": "Amazon",
    "developer associate": "Amazon",
}


@lru_cache(maxsize=128)
def normalize_vendor(text_to_search: str, exam_code: str = "") -> str:
    """
    Normalizes a vendor name from a given string, including exam code/name.
    """
    if not text_to_search:
        return "Unknown"

    lower_text = text_to_search.lower()
    exam_code_lower = exam_code.lower()

    # Check aliases first (these are high-confidence)
    for alias, canonical_name in VENDOR_ALIASES.items():
        if alias in lower_text:
            return canonical_name
    
    if exam_code_lower.startswith("300-"):
        return "Cisco"

    # Then try full canonical list using loose match
    for vendor in CANONICAL_VENDORS:
        if vendor.lower() in lower_text:
            return vendor

    return "Unknown"