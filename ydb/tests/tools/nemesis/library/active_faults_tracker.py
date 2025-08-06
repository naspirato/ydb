# -*- coding: utf-8 -*-
import threading
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ActiveFault:
    """Класс для представления активного нарушения"""
    
    def __init__(self, nemesis_name: str, fault_type: str, target: str, 
                 inject_time: datetime, description: str = ""):
        self.nemesis_name = nemesis_name
        self.fault_type = fault_type
        self.target = target
        self.inject_time = inject_time
        self.description = description
        self.extract_time: Optional[datetime] = None
        self.status = "active"  # active, extracted, failed
    
    def to_dict(self):
        """Преобразует нарушение в словарь для JSON"""
        return {
            "nemesis_name": self.nemesis_name,
            "fault_type": self.fault_type,
            "target": self.target,
            "inject_time": self.inject_time.isoformat(),
            "extract_time": self.extract_time.isoformat() if self.extract_time else None,
            "description": self.description,
            "status": self.status,
            "duration_seconds": int((datetime.now() - self.inject_time).total_seconds()) if self.status == "active" else 
                              int((self.extract_time - self.inject_time).total_seconds()) if self.extract_time else None
        }
    
    def __str__(self):
        return f"{self.nemesis_name}({self.fault_type}) on {self.target}"


class ActiveFaultsTracker:
    """Трекер активных нарушений"""
    
    def __init__(self):
        self._active_faults: Dict[str, ActiveFault] = {}
        self._lock = threading.RLock()
        self._fault_counter = 0
    
    def register_fault_injection(self, nemesis_name: str, fault_type: str, 
                                target: str, description: str = "") -> str:
        """Регистрирует инъекцию нарушения"""
        with self._lock:
            fault_id = f"{nemesis_name}_{self._fault_counter}_{int(time.time())}"
            self._fault_counter += 1
            
            fault = ActiveFault(
                nemesis_name=nemesis_name,
                fault_type=fault_type,
                target=target,
                inject_time=datetime.now(),
                description=description
            )
            
            self._active_faults[fault_id] = fault
            logger.info(f"Registered fault injection: {fault}")
            return fault_id
    
    def register_fault_extraction(self, nemesis_name: str, fault_type: str, 
                                 target: str, success: bool = True) -> bool:
        """Регистрирует извлечение нарушения"""
        with self._lock:
            # Ищем активное нарушение по nemesis_name и target
            fault_id = None
            for fid, fault in self._active_faults.items():
                if (fault.nemesis_name == nemesis_name and 
                    fault.target == target and 
                    fault.status == "active"):
                    fault_id = fid
                    break
            
            if fault_id:
                fault = self._active_faults[fault_id]
                fault.extract_time = datetime.now()
                fault.status = "extracted" if success else "failed"
                logger.info(f"Registered fault extraction: {fault} (success={success})")
                return True
            else:
                logger.warning(f"No active fault found for extraction: {nemesis_name} on {target}")
                return False
    
    def get_active_faults(self) -> List[Dict]:
        """Возвращает список активных нарушений"""
        with self._lock:
            active_faults = []
            for fault_id, fault in self._active_faults.items():
                if fault.status == "active":
                    fault_dict = fault.to_dict()
                    fault_dict["fault_id"] = fault_id
                    active_faults.append(fault_dict)
            return active_faults
    
    def get_all_faults(self, limit: int = 100) -> List[Dict]:
        """Возвращает все нарушения (активные и завершенные)"""
        with self._lock:
            all_faults = []
            for fault_id, fault in self._active_faults.items():
                fault_dict = fault.to_dict()
                fault_dict["fault_id"] = fault_id
                all_faults.append(fault_dict)
            
            # Сортируем по времени инъекции (новые сначала)
            all_faults.sort(key=lambda x: x["inject_time"], reverse=True)
            return all_faults[:limit]
    
    def cleanup_old_faults(self, max_age_hours: int = 24):
        """Удаляет старые завершенные нарушения"""
        with self._lock:
            cutoff_time = datetime.now().timestamp() - (max_age_hours * 3600)
            to_remove = []
            
            for fault_id, fault in self._active_faults.items():
                if (fault.status != "active" and 
                    fault.inject_time.timestamp() < cutoff_time):
                    to_remove.append(fault_id)
            
            for fault_id in to_remove:
                del self._active_faults[fault_id]
            
            if to_remove:
                logger.info(f"Cleaned up {len(to_remove)} old faults")


# Глобальный экземпляр трекера
_global_tracker = ActiveFaultsTracker()


def get_tracker() -> ActiveFaultsTracker:
    """Возвращает глобальный трекер нарушений"""
    return _global_tracker


def register_fault_injection(nemesis_name: str, fault_type: str, 
                           target: str, description: str = "") -> str:
    """Регистрирует инъекцию нарушения"""
    return _global_tracker.register_fault_injection(nemesis_name, fault_type, target, description)


def register_fault_extraction(nemesis_name: str, fault_type: str, 
                            target: str, success: bool = True) -> bool:
    """Регистрирует извлечение нарушения"""
    return _global_tracker.register_fault_extraction(nemesis_name, fault_type, target, success) 