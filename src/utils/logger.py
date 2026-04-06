import logging
import time
from datetime import datetime
import os
from contextlib import contextmanager

class PipelineLogger:
    def __init__(self):
        os.makedirs("logs", exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = f"logs/execution_{self.timestamp}.md"
        
        # 1. Crear un logger específico (aislado del resto del sistema)
        self.logger = logging.getLogger("PipelineLogger")
        self.logger.setLevel(logging.INFO)
        
        # 2. El antídoto: Limpiar handlers anteriores para evitar mensajes duplicados
        # y asegurar que nuestros nuevos handlers se apliquen.
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            
        # 3. Crear y configurar los handlers directamente
        formatter = logging.Formatter("%(message)s")
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        # Encabezado bonito
        self.logger.info(f"# 🚀 Ejecución Pipeline - {self.timestamp}\n")
        self.logger.info(f"**Inicio:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.total_start = time.time()

    @contextmanager
    def step(self, name: str):
        """Context manager que mide tiempo de cada paso usando generadores"""
        start = time.time()
        self.logger.info(f"### ⏳ {name} - Iniciando...")
        try:
            yield self  # Aquí es donde se ejecuta el código dentro de tu bloque 'with'
        finally:
            duration = time.time() - start
            self.logger.info(f"✅ **{name}** completado en **{duration:.2f} segundos**\n")

    def info(self, msg: str):
        self.logger.info(f"ℹ️ {msg}")

    def success(self, msg: str):
        self.logger.info(f"🎉 {msg}")

    def warning(self, msg: str):
        self.logger.info(f"⚠️ {msg}")

    def error(self, msg: str):
        self.logger.info(f"❌ {msg}")

    def finish(self):
        total_time = time.time() - self.total_start
        self.logger.info(f"\n---\n**✅ Pipeline finalizado con éxito en {total_time:.2f} segundos**")
        self.logger.info(f"📄 Log guardado en: `{self.log_file}`")