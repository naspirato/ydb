# -*- coding: utf-8 -*-
"""
Wrapper для автоматического добавления отслеживания к существующим nemesis
Позволяет отслеживать нарушения без переписывания существующего кода
"""

import logging
import functools
from ydb.tests.library.nemesis.nemesis_core import Nemesis
from .active_faults_tracker import register_fault_injection, register_fault_extraction

logger = logging.getLogger(__name__)


class NemesisTrackerWrapper:
    """Wrapper для добавления отслеживания к существующим nemesis"""
    
    def __init__(self, nemesis_instance: Nemesis, fault_type: str = None, 
                 target_extractor=None, description_extractor=None):
        """
        Args:
            nemesis_instance: Существующий экземпляр nemesis
            fault_type: Тип нарушения (если None, используется имя класса)
            target_extractor: Функция для извлечения цели нарушения
            description_extractor: Функция для извлечения описания нарушения
        """
        self._nemesis = nemesis_instance
        self._fault_type = fault_type or nemesis_instance.__class__.__name__
        self._target_extractor = target_extractor
        self._description_extractor = description_extractor
        self._current_target = None
        self._current_description = None
        
        # Сохраняем оригинальные методы
        self._original_inject_fault = nemesis_instance.inject_fault
        self._original_extract_fault = nemesis_instance.extract_fault
        
        # Заменяем методы на отслеживаемые версии
        self._nemesis.inject_fault = self._tracked_inject_fault
        self._nemesis.extract_fault = self._tracked_extract_fault
    
    def _get_target(self) -> str:
        """Извлекает цель нарушения"""
        if self._target_extractor:
            try:
                return self._target_extractor(self._nemesis)
            except Exception as e:
                logger.warning(f"Failed to extract target: {e}")
        
        # Попытка автоматического извлечения цели
        if hasattr(self._nemesis, '_current_pile_id'):
            return f"pile_{getattr(self._nemesis, '_current_pile_id', 'unknown')}"
        elif hasattr(self._nemesis, '_current_dc'):
            return f"dc_{getattr(self._nemesis, '_current_dc', 'unknown')}"
        elif hasattr(self._nemesis, '_current_node'):
            node = getattr(self._nemesis, '_current_node', None)
            return node.host if node else "unknown"
        elif hasattr(self._nemesis, 'cluster') and hasattr(self._nemesis.cluster, 'nodes'):
            return f"cluster_{len(self._nemesis.cluster.nodes)}_nodes"
        else:
            return "unknown"
    
    def _get_description(self) -> str:
        """Извлекает описание нарушения"""
        if self._description_extractor:
            try:
                return self._description_extractor(self._nemesis)
            except Exception as e:
                logger.warning(f"Failed to extract description: {e}")
        
        # Попытка автоматического извлечения описания
        nemesis_name = self._nemesis.__class__.__name__
        
        if hasattr(self._nemesis, '_current_nodes') and self._nemesis._current_nodes:
            node_hosts = [node.host for node in self._nemesis._current_nodes]
            return f"{nemesis_name}: {len(node_hosts)} nodes ({', '.join(node_hosts[:3])})"
        elif hasattr(self._nemesis, '_current_pile_id'):
            pile_id = getattr(self._nemesis, '_current_pile_id', 'unknown')
            return f"{nemesis_name}: pile {pile_id}"
        elif hasattr(self._nemesis, '_current_dc'):
            dc = getattr(self._nemesis, '_current_dc', 'unknown')
            return f"{nemesis_name}: datacenter {dc}"
        else:
            return f"{nemesis_name}: fault injection"
    
    def _tracked_inject_fault(self):
        """Отслеживаемая версия inject_fault"""
        try:
            # Получаем информацию о нарушении
            target = self._get_target()
            description = self._get_description()
            
            # Выполняем оригинальную инъекцию
            self._original_inject_fault()
            
            # Регистрируем нарушение
            register_fault_injection(
                nemesis_name=self._nemesis.__class__.__name__,
                fault_type=self._fault_type,
                target=target,
                description=description
            )
            
            self._current_target = target
            self._current_description = description
            
            logger.info(f"Fault injected and tracked: {self._nemesis.__class__.__name__} on {target}")
            
        except Exception as e:
            logger.error(f"Failed to inject fault: {e}")
            raise
    
    def _tracked_extract_fault(self):
        """Отслеживаемая версия extract_fault"""
        try:
            # Выполняем оригинальное извлечение
            self._original_extract_fault()
            
            # Регистрируем извлечение
            if self._current_target:
                success = register_fault_extraction(
                    nemesis_name=self._nemesis.__class__.__name__,
                    fault_type=self._fault_type,
                    target=self._current_target,
                    success=True
                )
                if success:
                    logger.info(f"Fault extracted and tracked: {self._nemesis.__class__.__name__} on {self._current_target}")
                else:
                    logger.warning(f"Failed to track fault extraction: {self._nemesis.__class__.__name__} on {self._current_target}")
            
            # Сбрасываем состояние
            self._current_target = None
            self._current_description = None
            
        except Exception as e:
            logger.error(f"Failed to extract fault: {e}")
            # Пытаемся зарегистрировать неудачное извлечение
            if self._current_target:
                register_fault_extraction(
                    nemesis_name=self._nemesis.__class__.__name__,
                    fault_type=self._fault_type,
                    target=self._current_target,
                    success=False
                )
            raise


def track_nemesis(nemesis_instance: Nemesis, fault_type: str = None, 
                 target_extractor=None, description_extractor=None) -> Nemesis:
    """
    Декоратор для добавления отслеживания к nemesis
    
    Args:
        nemesis_instance: Экземпляр nemesis для отслеживания
        fault_type: Тип нарушения
        target_extractor: Функция для извлечения цели (nemesis) -> str
        description_extractor: Функция для извлечения описания (nemesis) -> str
    
    Returns:
        Тот же экземпляр nemesis с добавленным отслеживанием
    """
    wrapper = NemesisTrackerWrapper(nemesis_instance, fault_type, target_extractor, description_extractor)
    return nemesis_instance


def track_nemesis_list(nemesis_list, **kwargs):
    """
    Добавляет отслеживание к списку nemesis
    
    Args:
        nemesis_list: Список nemesis для отслеживания
        **kwargs: Аргументы для track_nemesis
    
    Returns:
        Список nemesis с добавленным отслеживанием
    """
    tracked_list = []
    for nemesis in nemesis_list:
        tracked_nemesis = track_nemesis(nemesis, **kwargs)
        tracked_list.append(tracked_nemesis)
    return tracked_list


# Предопределенные экстракторы для популярных типов nemesis
def bridge_pile_target_extractor(nemesis):
    """Экстрактор цели для bridge pile nemesis"""
    if hasattr(nemesis, '_current_pile_id') and nemesis._current_pile_id is not None:
        return f"pile_{nemesis._current_pile_id}"
    return "bridge_pile_unknown"


def bridge_pile_description_extractor(nemesis):
    """Экстрактор описания для bridge pile nemesis"""
    nemesis_name = nemesis.__class__.__name__
    if hasattr(nemesis, '_current_nodes') and nemesis._current_nodes:
        node_hosts = [node.host for node in nemesis._current_nodes]
        return f"{nemesis_name}: {len(node_hosts)} nodes ({', '.join(node_hosts[:3])})"
    elif hasattr(nemesis, '_current_pile_id'):
        pile_id = nemesis._current_pile_id
        return f"{nemesis_name}: pile {pile_id}"
    return f"{nemesis_name}: bridge pile fault"


def datacenter_target_extractor(nemesis):
    """Экстрактор цели для datacenter nemesis"""
    if hasattr(nemesis, '_current_dc') and nemesis._current_dc is not None:
        return f"dc_{nemesis._current_dc}"
    return "datacenter_unknown"


def datacenter_description_extractor(nemesis):
    """Экстрактор описания для datacenter nemesis"""
    nemesis_name = nemesis.__class__.__name__
    if hasattr(nemesis, '_current_dc'):
        dc = nemesis._current_dc
        return f"{nemesis_name}: datacenter {dc}"
    return f"{nemesis_name}: datacenter fault"


def node_target_extractor(nemesis):
    """Экстрактор цели для node nemesis"""
    if hasattr(nemesis, '_current_node') and nemesis._current_node is not None:
        return nemesis._current_node.host
    return "node_unknown"


def node_description_extractor(nemesis):
    """Экстрактор описания для node nemesis"""
    nemesis_name = nemesis.__class__.__name__
    if hasattr(nemesis, '_current_node') and nemesis._current_node is not None:
        node = nemesis._current_node
        return f"{nemesis_name}: node {node.host}"
    return f"{nemesis_name}: node fault" 