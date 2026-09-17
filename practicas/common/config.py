# Databricks notebook source
"""Configuración segura y repetible para las prácticas en Databricks."""

from dataclasses import dataclass
import re


SCALE_ROWS = {
    "test": {"customers": 100, "products": 30, "transactions": 1_000, "events": 3_000},
    "small": {"customers": 5_000, "products": 500, "transactions": 50_000, "events": 200_000},
    "demo": {"customers": 20_000, "products": 2_000, "transactions": 250_000, "events": 1_000_000},
}


def normalize_student_id(value: str) -> str:
    """Convierte un identificador humano en un identificador SQL controlado."""
    normalized = re.sub(r"[^a-z0-9_]", "_", value.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized or not re.fullmatch(r"[a-z][a-z0-9_]{1,30}", normalized):
        raise ValueError(
            "Usá entre 2 y 31 caracteres: comenzar con letra y continuar con letras, números o _."
        )
    return normalized


def quote_identifier(value: str) -> str:
    """Cita un identificador previamente validado."""
    if "`" in value:
        raise ValueError("Identificador SQL inválido")
    return f"`{value}`"


@dataclass(frozen=True)
class CourseConfig:
    catalog: str
    student_id: str
    scale: str = "small"
    seed: int = 39758307

    def __post_init__(self):
        object.__setattr__(self, "student_id", normalize_student_id(self.student_id))
        if self.scale not in SCALE_ROWS:
            raise ValueError(f"Escala inválida: {self.scale}. Opciones: {sorted(SCALE_ROWS)}")
        if not self.catalog or "`" in self.catalog:
            raise ValueError("Catálogo inválido")

    @property
    def schema(self) -> str:
        return f"bigdata_{self.student_id}"

    @property
    def namespace(self) -> str:
        return f"{quote_identifier(self.catalog)}.{quote_identifier(self.schema)}"

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/landing"

    @property
    def rows(self) -> dict:
        return SCALE_ROWS[self.scale].copy()


def create_course_namespace(spark, config: CourseConfig) -> None:
    """Crea solamente el esquema y volumen administrado del alumno."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.namespace}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.namespace}.`landing`")
    spark.sql(f"USE CATALOG {quote_identifier(config.catalog)}")
    spark.sql(f"USE SCHEMA {quote_identifier(config.schema)}")

