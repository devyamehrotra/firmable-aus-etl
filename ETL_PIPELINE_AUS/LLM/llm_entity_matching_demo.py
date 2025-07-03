import openai

# Set your OpenAI API key
openai.api_key = "sk-..."  # <-- Replace with your key or use os.environ

# Example company records
abr_company = {
    "abn": "12345678901",
    "entity_name": "Acme Pty Ltd",
    "entity_type": "Proprietary Company",
    "entity_status": "Active",
    "address_line1": "123 Main St",
    "suburb": "Sydney",
    "state": "NSW",
    "postcode": "2000"
}

cc_company = {
    "company_name": "Acme Pty Limited",
    "website_url": "www.acme.com.au",
    "industry": "Manufacturing",
    "address": "123 Main Street, Sydney, NSW 2000"
}

# LLM prompt for entity matching
prompt = f"""
You are an expert in company data matching. Given two company records, determine if they refer to the same company. 
Return your answer as JSON with fields: "match" (true/false), "confidence" (0-1), and "reason".

ABR company:
{abr_company}

Common Crawl company:
{cc_company}
"""

# Calling  the LLM
response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",  # or "gpt-4" if you have access
    messages=[
        {"role": "system", "content": "You are a helpful assistant for entity resolution."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.2,
    max_tokens=300
)

# Print the LLM's response
print("LLM Output:")
print(response['choices'][0]['message']['content'])