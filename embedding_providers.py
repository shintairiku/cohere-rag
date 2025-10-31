import base64
import inspect
import os
import tempfile
from abc import ABC, abstractmethod
from typing import Optional

import numpy as np

try:
    import cohere  # type: ignore
except ImportError:  # pragma: no cover
    cohere = None

try:
    import vertexai  # type: ignore
    from vertexai.preview.vision_models import MultiModalEmbeddingModel, Image as VertexImage  # type: ignore
except ImportError:  # pragma: no cover
    vertexai = None
    MultiModalEmbeddingModel = None
    VertexImage = None


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""

    provider_name: str
    display_name: str

    @abstractmethod
    def embed_multimodal(
        self,
        *,
        text: str,
        image_bytes: Optional[bytes],
        use_embed_v4: bool = False,
    ) -> np.ndarray:
        """Generate an embedding that represents both text and image information."""

    @abstractmethod
    def embed_text(
        self,
        *,
        text: str,
        use_embed_v4: bool = False,
    ) -> np.ndarray:
        """Generate an embedding for text-only input."""


class VertexEmbeddingProvider(EmbeddingProvider):
    """Vertex AI multi-modal embedding provider."""

    def __init__(self) -> None:
        if vertexai is None or MultiModalEmbeddingModel is None or VertexImage is None:
            raise ImportError("vertexai package is required for VertexEmbeddingProvider")

        self.provider_name = "vertex_ai"
        self.display_name = "Vertex AI"
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.location = os.getenv("GCP_REGION", "us-central1")
        self.model_name = os.getenv("VERTEX_MULTIMODAL_MODEL", "multimodalembedding@001")

        if not self.project_id:
            raise RuntimeError("GCP_PROJECT_ID must be set when using Vertex AI embeddings")

        vertexai.init(project=self.project_id, location=self.location)
        self._model = MultiModalEmbeddingModel.from_pretrained(self.model_name)
        self._dimension: Optional[int] = None
        self._embedding_params = inspect.signature(self._model.get_embeddings).parameters

    def _call_get_embeddings(self, *, image=None, text: Optional[str] = None):
        kwargs = {}
        if image is not None:
            if "image" in self._embedding_params:
                kwargs["image"] = image
            elif "image_input" in self._embedding_params:
                kwargs["image_input"] = image
            else:
                raise RuntimeError("Vertex AI client does not accept image input parameter")

        if text is not None:
            if image is not None and "contextual_text" in self._embedding_params:
                kwargs["contextual_text"] = text
            elif "text" in self._embedding_params:
                kwargs["text"] = text
            elif "text_input" in self._embedding_params:
                kwargs["text_input"] = text
            else:
                raise RuntimeError("Vertex AI client does not accept text input parameter")

        if not kwargs:
            raise ValueError("No arguments provided to Vertex AI get_embeddings")

        return self._model.get_embeddings(**kwargs)

    def embed_text(
        self,
        *,
        text: str,
        use_embed_v4: bool = False,
    ) -> np.ndarray:
        if use_embed_v4:
            print("⚠️  USE_EMBED_V4 is ignored by the Vertex AI provider.")

        print(f"    🔧 {self.display_name}: Generating text embedding with model '{self.model_name}'")
        embeddings = self._call_get_embeddings(text=text)
        text_embedding = getattr(embeddings, "text_embedding", None)
        if not text_embedding:
            raise ValueError("Vertex AI returned empty text embedding")

        vec = np.asarray(text_embedding, dtype=np.float32)
        self._dimension = len(vec)
        return vec

    def embed_multimodal(
        self,
        *,
        text: str,
        image_bytes: Optional[bytes],
        use_embed_v4: bool = False,
    ) -> np.ndarray:
        if use_embed_v4:
            print("⚠️  USE_EMBED_V4 is ignored by the Vertex AI provider.")

        if not image_bytes:
            return self.embed_text(text=text, use_embed_v4=use_embed_v4)

        suffix = _infer_file_suffix(text)
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp_file:
            tmp_file.write(image_bytes)
            tmp_file.flush()
            temp_path = tmp_file.name

        try:
            vertex_image = VertexImage.load_from_file(temp_path)
        finally:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass

        print(f"    🔧 {self.display_name}: Generating multimodal embedding with model '{self.model_name}'")
        embeddings = self._call_get_embeddings(image=vertex_image, text=text)

        image_embedding = getattr(embeddings, "image_embedding", None)
        if not image_embedding:
            raise ValueError("Vertex AI returned empty image embedding")
        image_vec = np.asarray(image_embedding, dtype=np.float32)

        text_embedding = getattr(embeddings, "text_embedding", None)
        if text_embedding:
            text_vec = np.asarray(text_embedding, dtype=np.float32)
        else:
            text_vec = image_vec.copy()

        image_vec, text_vec = _align_dimensions(image_vec, text_vec)
        dot_product = float(np.dot(text_vec, image_vec))
        norm_text = float(np.linalg.norm(text_vec))
        norm_image = float(np.linalg.norm(image_vec))

        if norm_text == 0 or norm_image == 0:
            weight = 0.5
        else:
            weight = max(0.0, min(1.0, dot_product / (norm_text * norm_image)))

        final_vec = weight * text_vec + (1.0 - weight) * image_vec
        print(f"    📊 Text-Image similarity: {weight:.3f} (Vertex AI)")

        self._dimension = len(final_vec)
        return final_vec.astype(np.float32)


class CohereEmbeddingProvider(EmbeddingProvider):
    """Cohere embed API provider."""

    def __init__(self) -> None:
        if cohere is None:
            raise ImportError("cohere package is required for CohereEmbeddingProvider")

        self.provider_name = "cohere"
        self.display_name = "Cohere"
        self.api_key = os.getenv("COHERE_API_KEY")
        if not self.api_key:
            raise RuntimeError("COHERE_API_KEY must be set when using the Cohere embedding provider")

        self._client = cohere.Client(self.api_key)
        self.default_model = os.getenv("COHERE_EMBED_MODEL_DOCUMENT", "embed-multilingual-v3.0")
        self.v4_model = os.getenv("COHERE_EMBED_MODEL_V4", "embed-v4.0")

    def _resolve_model(self, use_embed_v4: bool) -> str:
        return self.v4_model if use_embed_v4 else self.default_model

    def embed_text(
        self,
        *,
        text: str,
        use_embed_v4: bool = False,
    ) -> np.ndarray:
        model = self._resolve_model(use_embed_v4)

        print(f"    🔧 {self.display_name}: Generating text embedding with model '{model}'")
        response = self._client.embed(
            texts=[text],
            model=model,
            input_type="search_query",
        )
        vec = np.asarray(response.embeddings[0], dtype=np.float32)
        return vec

    def embed_multimodal(
        self,
        *,
        text: str,
        image_bytes: Optional[bytes],
        use_embed_v4: bool = False,
    ) -> np.ndarray:
        model = self._resolve_model(use_embed_v4)

        if not image_bytes:
            return self.embed_text(text=text, use_embed_v4=use_embed_v4)

        print(f"    🔧 {self.display_name}: Generating multimodal embedding with model '{model}'")

        text_response = self._client.embed(
            texts=[text],
            model=model,
            input_type="search_document",
        )
        text_vec = np.asarray(text_response.embeddings[0], dtype=np.float32)

        mime_type = _infer_mime_type(text)
        base64_string = base64.b64encode(image_bytes).decode("utf-8")
        data_uri = f"data:image/{mime_type};base64,{base64_string}"

        image_response = self._client.embed(
            images=[data_uri],
            model=model,
            input_type="image",
        )
        image_vec = np.asarray(image_response.embeddings[0], dtype=np.float32)

        image_vec, text_vec = _align_dimensions(image_vec, text_vec)
        dot_product = float(np.dot(text_vec, image_vec))
        norm_text = float(np.linalg.norm(text_vec))
        norm_image = float(np.linalg.norm(image_vec))

        if norm_text == 0 or norm_image == 0:
            weight = 0.5
        else:
            weight = max(0.0, min(1.0, dot_product / (norm_text * norm_image)))

        final_vec = weight * text_vec + (1.0 - weight) * image_vec
        print(f"    📊 Text-Image similarity: {weight:.3f} (Cohere)")

        return final_vec.astype(np.float32)


_PROVIDER_CACHE: Optional[EmbeddingProvider] = None


def get_embedding_provider(force_reload: bool = False) -> EmbeddingProvider:
    """Return a cached embedding provider instance based on environment configuration."""
    global _PROVIDER_CACHE

    if force_reload:
        _PROVIDER_CACHE = None

    if _PROVIDER_CACHE is not None:
        return _PROVIDER_CACHE

    provider_name = os.getenv("EMBEDDING_PROVIDER", "vertex_ai").lower()

    if provider_name == "vertex_ai":
        _PROVIDER_CACHE = VertexEmbeddingProvider()
    elif provider_name == "cohere":
        _PROVIDER_CACHE = CohereEmbeddingProvider()
    else:
        raise ValueError(f"Unsupported EMBEDDING_PROVIDER: {provider_name}")

    print(f"🔗 Embedding provider initialized: {_PROVIDER_CACHE.display_name}")
    return _PROVIDER_CACHE


def _infer_file_suffix(filename: str) -> str:
    ext = filename.lower().split(".")[-1]
    if ext in {"jpg", "jpeg"}:
        return ".jpg"
    if ext == "png":
        return ".png"
    if ext == "gif":
        return ".gif"
    if ext == "webp":
        return ".webp"
    return ".jpg"


def _infer_mime_type(filename: str) -> str:
    ext = filename.lower().split(".")[-1]
    if ext in {"jpg", "jpeg"}:
        return "jpeg"
    if ext == "png":
        return "png"
    if ext == "gif":
        return "gif"
    if ext == "webp":
        return "webp"
    return "jpeg"


def _align_dimensions(image_vec: np.ndarray, text_vec: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Ensure the two vectors share the same dimensionality."""
    if image_vec.shape == text_vec.shape:
        return image_vec, text_vec

    min_dim = min(image_vec.shape[0], text_vec.shape[0])
    return image_vec[:min_dim], text_vec[:min_dim]
