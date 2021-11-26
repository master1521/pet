import logging
from datetime import datetime
from random import randint

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('account')


# ПРИМЕР 1 Банковский счет
class Account:
    yam = 999999999999999999

    def __init__(self, name: str, balance: int):
        self.name = name
        self.__balance = balance  # Чтобы нельзя было обратится напрямую  a.__balance = 10000000000
        self.currency = f"$"
        self._history = []  # История должна быть доступна только самому пользователю

    def show_balance(self):
        """ Показывает баланс счета """

        logger.info(f" Баланс вашего счета {self.__balance}{self.currency}")
        return self.__balance

    def add(self, amount: int):
        """ Добавляет рублики на счет """

        self._history.append(f"{datetime.now()} Пополнение: +{amount}$")
        self.__balance += amount
        logger.info(f" Добавлено +{amount}{self.currency}")
        self.show_balance()

    def send(self, amount: int):
        """ Списывает рублики со счета """

        if self.__balance > amount:
            self._history.append(f"{datetime.now()} Списание: -{amount}$")
            self.__balance -= amount
            logger.info(f" Списано со счета -{amount}{self.currency}")
            self.show_balance()
        else:
            logger.error(f" Не удалось списать {amount}{self.currency}! Недостаточно $ на счете")
            self.show_balance()

    def show_history(self):
        """ Показывает историю операций """

        logger.info("===== История операций =====:")
        for transaction in self._history:
            # print(transaction)
            logger.info(f"{transaction}")

    balance = property(show_balance, add)

# Тестовый запуск
a = Account('Ivan', 100)
a.balance
a.balance = 200
a.send(150)
a.show_history()

for i in range(10):
    if randint(1, 999) > 500:
        a.balance = randint(1, 1000)
    else:
        a.send(randint(1, 1000))
a.show_history()

