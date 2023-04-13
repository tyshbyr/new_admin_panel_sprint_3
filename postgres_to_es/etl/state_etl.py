import abc
import json
from typing import Any, Optional


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def set_value(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def get_value(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def get_value(self) -> dict:
        try:
            with open(self.file_path, "r") as f:
                data = f.read()
                if not data:
                    return {}
                return json.loads(data)
        except FileNotFoundError:
            return {}

    def set_value(self, data: dict):
        data = json.dumps(data)
        with open(self.file_path, "w") as f:
            f.write(data)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно
    не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или
    распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        data = self.storage.get_value()
        data[key] = value
        self.storage.set_value(data)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        data = self.storage.get_value()
        if data:
            return data[key] if key in data.keys() else None
        else:
            return None
