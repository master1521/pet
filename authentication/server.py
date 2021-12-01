from fastapi import FastAPI, Form
from fastapi.responses import Response

""" Система аутентификации"""


app = FastAPI()


# Пока в место базы словарь
users = {
	"ivan@mail.ru": {
		"name": "Ivan",
		"password": "hello123",
		"balance": 100000
	},
	"vika@gmail.com": {
		"name": "Vika",
		"password": "hello321",
		"balance": 300000

	}
}


@app.get("/")
def index_page():
	""" Главная страница """
	with open('templates/login.html', 'r') as file:
		login_page = file.read()
	return Response(login_page, media_type='text/html')


# Запускаем сервер в консоли из под виртуального окружения
# uvicorn server:app --reload

@app.post("/login")
def process_login_page(user_name: str = Form(...), user_password: str = Form(...)):
	""" Страница авторизации """
	user = users.get(user_name)

	# Вариант логики 1
	if user_name in users and user_password == user["password"]:
		# Добавляем куки для сохнанения сессии
		response = Response(f"Привет, {user['name']} \n Ваш баланса {user['balance']}", media_type='text/html')
		response.set_cookie(key="user_name", value=user_name)
		return response
	else:
		return Response('Я вас не знаю', media_type="text/html")

	# Вариант логики 2
	# if not user or user["password"] != user_password:
	# 	return Response('Я вас не знаю', media_type="text/html")

	# else:
	# 	# Добавляем куки для сохнанения сессии
	# 	response = Response(f"Привет, {user['name']} \n Ваш баланса {user['balance']}", media_type='text/html')
	# 	response.set_cookie(key="user_name", value=user_name)
	# 	return response