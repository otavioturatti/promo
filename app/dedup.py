"""Deduplicação de produtos por similaridade de nome (Jaccard de tokens)."""
import re
import unicodedata


def normalize_tokens(nome: str) -> set:
    """Tokens significativos do nome: sem acento, minúsculas, palavras com >= 3 chars."""
    if not nome:
        return set()
    nfkd = unicodedata.normalize("NFD", nome)
    sem_acento = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    limpo = re.sub(r"[^a-z0-9]+", " ", sem_acento.lower())
    return {t for t in limpo.split() if len(t) >= 3}


def jaccard(a: set, b: set) -> float:
    """Índice de Jaccard entre dois conjuntos de tokens (0.0 se algum for vazio)."""
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def is_duplicate(nome: str, sent_names, threshold: float = 0.5) -> bool:
    """True se `nome` for similar (Jaccard >= threshold) a algum nome em `sent_names`."""
    toks = normalize_tokens(nome)
    if not toks:
        return False
    return any(jaccard(toks, normalize_tokens(s)) >= threshold for s in sent_names)
