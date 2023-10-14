import requests
import json

url = "https://api.spoonacular.com/food/products/search?query=pizza&number=2"
api_key = "be1b3a05ae514d49bc49ace1237693c6"
params = {"IncludeIngredients": "true", "apiKey": api_key}

response = requests.get(url, params=params)

data = response.json()
# print(data)




with open("recipe.json", "w") as outfile:
    json.dump(data, outfile)

# with open("recipe.json", "w") as outfile:
#     for item in data:
#         json.dump(item, outfile)
#         outfile.write('\n')