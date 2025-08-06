# -*- coding: utf-8 -*-
import logging
from abc import abstractmethod
from ydb.tests.library.nemesis.nemesis_core import Nemesis
from .active_faults_tracker import register_fault_injection, register_fault_extraction

logger = logging.getLogger(__name__)


class TrackedNemesis(Nemesis):
    """Базовый класс для nemesis с автоматическим отслеживанием нарушений"""
    
    def __init__(self, schedule):
        super(TrackedNemesis, self).__init__(schedule)
        self._current_fault_id = None
        self._current_target = None
        self._current_fault_type = None
    
    @abstractmethod
    def get_fault_type(self) -> str:
        """Возвращает тип нарушения"""
        pass
    
    @abstractmethod
    def get_target(self) -> str:
        """Возвращает цель нарушения (узел, порт, маршрут и т.д.)"""
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        """Возвращает описание нарушения"""
        pass
    
    def inject_fault(self):
        """Инъекция нарушения с автоматическим отслеживанием"""
        try:
            # Получаем информацию о нарушении
            fault_type = self.get_fault_type()
            target = self.get_target()
            description = self.get_description()
            
            # Выполняем инъекцию
            self._inject_fault_impl()
            
            # Регистрируем нарушение
            self._current_fault_id = register_fault_injection(
                nemesis_name=self.__class__.__name__,
                fault_type=fault_type,
                target=target,
                description=description
            )
            self._current_target = target
            self._current_fault_type = fault_type
            
            logger.info(f"Fault injected and tracked: {self.__class__.__name__} on {target}")
            
        except Exception as e:
            logger.error(f"Failed to inject fault: {e}")
            raise
    
    def extract_fault(self):
        """Извлечение нарушения с автоматическим отслеживанием"""
        try:
            # Выполняем извлечение
            self._extract_fault_impl()
            
            # Регистрируем извлечение
            if self._current_target and self._current_fault_type:
                success = register_fault_extraction(
                    nemesis_name=self.__class__.__name__,
                    fault_type=self._current_fault_type,
                    target=self._current_target,
                    success=True
                )
                if success:
                    logger.info(f"Fault extracted and tracked: {self.__class__.__name__} on {self._current_target}")
                else:
                    logger.warning(f"Failed to track fault extraction: {self.__class__.__name__} on {self._current_target}")
            
            # Сбрасываем состояние
            self._current_fault_id = None
            self._current_target = None
            self._current_fault_type = None
            
        except Exception as e:
            logger.error(f"Failed to extract fault: {e}")
            # Пытаемся зарегистрировать неудачное извлечение
            if self._current_target and self._current_fault_type:
                register_fault_extraction(
                    nemesis_name=self.__class__.__name__,
                    fault_type=self._current_fault_type,
                    target=self._current_target,
                    success=False
                )
            raise
    
    @abstractmethod
    def _inject_fault_impl(self):
        """Реальная реализация инъекции нарушения"""
        pass
    
    @abstractmethod
    def _extract_fault_impl(self):
        """Реальная реализация извлечения нарушения"""
        pass 