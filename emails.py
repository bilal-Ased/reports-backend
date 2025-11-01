import json
import uuid
import time
from datetime import datetime
import re
from html import unescape

# File paths
input_file = '/Users/bilalmughal/Documents/Dev/kati-reports-backend/emails.json'
output_file = '/Users/bilalmughal/Documents/Dev/kati-reports-backend/filtered_emails.json'

def clean_json_content(content):
    """Clean and fix malformed JSON content"""
    content = content.strip()
    if content.startswith('},'):
        content = content[2:].strip()
    
    if not content.startswith('[') and not content.startswith('{'):
        content = '[' + content
    
    if content.endswith('{'):
        content = content[:-1]
    
    if not content.endswith(']') and not content.endswith('}'):
        content += ']'
    
    return content

def parse_json_safely(file_path):
    """Safely parse JSON file with potential formatting issues"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()
        
        try:
            data = json.loads(raw_content)
            return data
        except json.JSONDecodeError:
            print("⚠️ JSON parsing failed, attempting to fix format...")
            cleaned_content = clean_json_content(raw_content)
            
            try:
                data = json.loads(cleaned_content)
                return data
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON: {e}")
                return []
    
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return []

def extract_messages_from_nested_structure(data):
    """Extract email messages from the nested webhook structure"""
    messages = []
    
    def find_messages_recursive(obj, path=""):
        if isinstance(obj, dict):
            # Check if this looks like a message object
            if ('subject' in obj and 'from' in obj and 'to' in obj and 'id' in obj):
                messages.append(obj)
                print(f"📧 Found message at {path}: {obj.get('subject', 'No Subject')}")
            else:
                # Recursively search nested objects
                for key, value in obj.items():
                    find_messages_recursive(value, f"{path}.{key}" if path else key)
        elif isinstance(obj, list):
            # Search through list items
            for i, item in enumerate(obj):
                find_messages_recursive(item, f"{path}[{i}]" if path else f"[{i}]")
    
    if isinstance(data, list):
        for i, item in enumerate(data):
            find_messages_recursive(item, f"root[{i}]")
    else:
        find_messages_recursive(data, "root")
    
    return messages

# Load and parse JSON
print(f"📥 Loading JSON from {input_file}")
raw_data = parse_json_safely(input_file)

if not raw_data:
    print("❌ No data loaded. Please check your input file.")
    exit(1)

# Extract all email messages from nested structure
print(f"🔍 Searching for email messages in nested structure...")
data = extract_messages_from_nested_structure(raw_data)

total_before = len(data)
print(f"📊 Found {total_before} total email messages")

if total_before == 0:
    print("❌ No email messages found. Please check your file structure.")
    exit(1)

# Filter out messages from contactcentre@dtbafrica.com
filtered_messages = []
blocked_count = 0

print(f"🔄 Filtering messages...")
for item in data:
    if not isinstance(item, dict):
        continue
        
    is_blocked = False
    from_field = item.get("from", [])
    
    if isinstance(from_field, list):
        for sender in from_field:
            if isinstance(sender, dict) and sender.get("email") == "contactcentre@dtbafrica.com":
                is_blocked = True
                blocked_count += 1
                print(f"🚫 Blocked: {item.get('subject', 'No Subject')}")
                break
    
    if not is_blocked:
        filtered_messages.append(item)

print(f"✅ Kept {len(filtered_messages)} messages after filtering")

def format_timestamp(unix_timestamp):
    """Convert Unix timestamp to ISO 8601 format"""
    try:
        if isinstance(unix_timestamp, (int, float)):
            return datetime.fromtimestamp(unix_timestamp).isoformat() + 'Z'
        return datetime.now().isoformat() + 'Z'
    except:
        return datetime.now().isoformat() + 'Z'

def clean_html(html_content):
    """Extract clean text from HTML content"""
    if not html_content:
        return ""
    
    # Remove script and style elements
    html_content = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', html_content)
    
    # Decode HTML entities
    text = unescape(text)
    
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove tracking content
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        if (len(line) < 200 and 
            not line.startswith('http') and 
            'tracking' not in line.lower() and
            not line.startswith('TN1')):
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines).strip()

def extract_content(message):
    """Extract clean, readable content from email message"""
    content_parts = []
    
    # Subject
    subject = message.get('subject', '').strip()
    if subject:
        content_parts.append(f"Subject: {subject}")
    
    # From
    from_field = message.get('from', [])
    if isinstance(from_field, list) and len(from_field) > 0:
        sender = from_field[0]
        if isinstance(sender, dict):
            name = sender.get('name', '').strip()
            email = sender.get('email', '').strip()
            if name and email:
                content_parts.append(f"From: {name} <{email}>")
            elif email:
                content_parts.append(f"From: {email}")
    
    # To
    to_field = message.get('to', [])
    if isinstance(to_field, list) and len(to_field) > 0:
        recipients = []
        for recipient in to_field[:3]:
            if isinstance(recipient, dict):
                name = recipient.get('name', '').strip()
                email = recipient.get('email', '').strip()
                if name and email:
                    recipients.append(f"{name} <{email}>")
                elif email:
                    recipients.append(email)
        if recipients:
            content_parts.append(f"To: {', '.join(recipients)}")
    
    # CC
    cc_field = message.get('cc', [])
    if isinstance(cc_field, list) and len(cc_field) > 0:
        cc_recipients = []
        for recipient in cc_field[:2]:
            if isinstance(recipient, dict):
                name = recipient.get('name', '').strip()
                email = recipient.get('email', '').strip()
                if name and email:
                    cc_recipients.append(f"{name} <{email}>")
                elif email:
                    cc_recipients.append(email)
        if cc_recipients:
            content_parts.append(f"CC: {', '.join(cc_recipients)}")
    
    # Body content - prefer snippet, then clean HTML
    body_content = ""
    snippet = message.get('snippet', '').strip()
    if snippet:
        body_content = snippet
    else:
        html_body = message.get('body', '')
        if html_body:
            body_content = clean_html(html_body)
    
    if body_content:
        if len(body_content) > 1000:
            body_content = body_content[:1000] + "..."
        content_parts.append(f"Content: {body_content}")
    
    # Date
    date_field = message.get('date')
    if date_field:
        try:
            if isinstance(date_field, (int, float)):
                date_str = datetime.fromtimestamp(date_field).strftime('%Y-%m-%d %H:%M:%S')
                content_parts.append(f"Date: {date_str}")
        except:
            pass
    
    # Attachments
    attachments = message.get('attachments', [])
    if isinstance(attachments, list) and len(attachments) > 0:
        non_inline_attachments = [att for att in attachments if isinstance(att, dict) and not att.get('is_inline', False)]
        if non_inline_attachments:
            attachment_names = []
            for att in non_inline_attachments[:3]:
                filename = att.get('filename', '').strip()
                if filename:
                    attachment_names.append(filename)
            if attachment_names:
                content_parts.append(f"Attachments: {', '.join(attachment_names)}")
    
    return "\n\n".join(content_parts)

# Process each message and add extracted content
webhook_payloads = []
processed_count = 0

print(f"\n🔄 Processing {len(filtered_messages)} messages...")

for i, message in enumerate(filtered_messages):
    try:
        # Extract readable content
        extracted_content = extract_content(message)
        
        # Create enhanced message with extracted content
        enhanced_message = message.copy()
        enhanced_message['extracted_content'] = extracted_content
        
        # Create webhook event
        webhook_event = {
            "specversion": "1.0",
            "type": "message.created",
            "source": "/microsoft/emails/realtime",
            "id": str(uuid.uuid4()),
            "time": format_timestamp(message.get("date")),
            "webhook_delivery_attempt": 1,
            "data": {
                "application_id": "your-nylas-app-id",
                "object": enhanced_message
            }
        }
        
        webhook_payloads.append(webhook_event)
        processed_count += 1
        
        # Progress indicator
        if (i + 1) % 5 == 0 or (i + 1) == len(filtered_messages):
            print(f"   ✅ Processed {i + 1}/{len(filtered_messages)} messages...")
            
    except Exception as e:
        print(f"⚠️ Error processing message {i + 1}: {e}")
        continue

# Save results
total_after = len(webhook_payloads)

try:
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(webhook_payloads, f, indent=4, ensure_ascii=False)
    print(f"💾 Successfully saved to: {output_file}")
except Exception as e:
    print(f"❌ Error saving file: {e}")
    exit(1)

# Summary
print(f"\n📊 PROCESSING SUMMARY:")
print(f"   📥 Total messages found:   {total_before}")
print(f"   🗑️  Messages filtered out:  {blocked_count}")
print(f"   ✅ Messages processed:     {processed_count}")
print(f"   💾 Webhook events created: {total_after}")

# Show sample results
if total_after > 0:
    print(f"\n📋 Sample of processed messages:")
    for i, payload in enumerate(webhook_payloads[:5]):
        msg = payload['data']['object']
        from_info = "Unknown sender"
        if isinstance(msg.get('from'), list) and len(msg['from']) > 0:
            sender = msg['from'][0]
            if isinstance(sender, dict):
                name = sender.get('name', '')
                email = sender.get('email', '')
                from_info = f"{name} <{email}>" if name else email
        
        subject = msg.get('subject', 'No Subject')
        if len(subject) > 50:
            subject = subject[:50] + "..."
        
        # Verify extracted_content exists
        has_content = 'extracted_content' in msg and len(msg['extracted_content']) > 0
        content_indicator = "✅" if has_content else "❌"
        
        print(f"   {i+1}. {content_indicator} {subject} - From: {from_info}")
    
    if total_after > 5:
        print(f"   ... and {total_after - 5} more messages")
    
    # Show first extracted content sample
    if total_after > 0:
        first_msg = webhook_payloads[0]['data']['object']
        if 'extracted_content' in first_msg:
            print(f"\n📝 Sample extracted content:")
            content_preview = first_msg['extracted_content'][:400]
            if len(first_msg['extracted_content']) > 400:
                content_preview += "..."
            print(f"   {content_preview}")
        else:
            print(f"\n❌ No extracted_content found in first message!")

print(f"\n✅ Processing complete!")