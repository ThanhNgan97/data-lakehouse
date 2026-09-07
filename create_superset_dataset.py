import requests, json

session = requests.Session()
login_payload = {'password': 'admin', 'provider': 'db', 'refresh': True, 'username': 'admin'}
r = session.post('http://localhost:8088/api/v1/security/login', json=login_payload)
token = r.json()['access_token']

r_csrf = session.get('http://localhost:8088/api/v1/security/csrf_token/', headers={'Authorization': f'Bearer {token}'})
csrf_token = r_csrf.json().get('result')

headers = {
    'Authorization': f'Bearer {token}',
    'X-CSRFToken': csrf_token,
    'Content-Type': 'application/json'
}

payload = {
  'database': 1,
  'schema': 'gold',
  'table_name': 'Danh_Sach_File_Nguon_Tat_Ca',
  'sql': "SELECT DISTINCT file_nguon AS file_name FROM kpi_tong_hop_don_vi UNION SELECT 'Tất cả' AS file_name"
}

res = session.post('http://localhost:8088/api/v1/dataset/', headers=headers, json=payload)
print(res.status_code)
print(res.text)
