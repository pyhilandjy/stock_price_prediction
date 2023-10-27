import requests

def get_public_ip():
    try:
        response = requests.get("https://httpbin.org/ip")
        if response.status_code == 200:
            data = response.json()
            return data["origin"]
        else:
            return "Unable to retrieve public IP"
    except Exception as e:
        return "Error: " + str(e)

if __name__ == "__main__":
    public_ip = get_public_ip()
    print("Your public IP address is:", public_ip)

