// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package wvspec

// APIBasedModules is the native API-based registration vocabulary shared by
// both supported release lines. Provider API keys remain request headers.
var APIBasedModules = []string{
	"text2vec-aws",
	"multi2vec-cohere",
	"text2vec-cohere",
	"text2vec-databricks",
	"text2vec-google",
	"multi2vec-google",
	"text2vec-huggingface",
	"text2vec-jinaai",
	"multi2vec-jinaai",
	"text2vec-mistral",
	"text2vec-octoai",
	"text2vec-openai",
	"generative-deepseek",
	"text2vec-digitalocean",
	"text2vec-morph",
	"text2vec-voyageai",
	"multi2vec-voyageai",
	"text2vec-weaviate",
	"text2multivec-jinaai",
	"text2vec-nvidia",
	"multi2vec-nvidia",
	"multi2vec-twelvelabs",
	"multi2multivec-jinaai",
	"multi2multivec-weaviate",
	"multi2vec-aws",
	"generative-anthropic",
	"generative-anyscale",
	"generative-aws",
	"generative-cohere",
	"generative-contextualai",
	"generative-databricks",
	"generative-digitalocean",
	"generative-friendliai",
	"generative-google",
	"generative-meta",
	"generative-mistral",
	"generative-nvidia",
	"generative-octoai",
	"generative-openai",
	"generative-xai",
	"reranker-cohere",
	"reranker-contextualai",
	"reranker-voyageai",
	"reranker-jinaai",
	"reranker-nvidia",
}

var inferenceModules = map[string]string{
	"TRANSFORMERS_INFERENCE_API": "text2vec-transformers",
	"CLIP_INFERENCE_API":         "multi2vec-clip",
	"RERANKER_INFERENCE_API":     "reranker-transformers",
	"BIND_INFERENCE_API":         "multi2vec-bind",
	"IMAGE_INFERENCE_API":        "img2vec-neural",
	"MODEL2VEC_INFERENCE_API":    "text2vec-model2vec",
	"GPT4ALL_INFERENCE_API":      "text2vec-gpt4all",
	"NER_INFERENCE_API":          "ner-transformers",
	"QNA_INFERENCE_API":          "qna-transformers",
	"SUM_INFERENCE_API":          "sum-transformers",
	"SPELLCHECK_INFERENCE_API":   "text-spellcheck",
}

// InferenceModule resolves only verified environment variables, never raw env.
// Ollama has collection-level apiEndpoint settings, not a process endpoint env;
// consequently Ollama modules are not supported by this endpoint contract.
func InferenceModule(env string) (string, bool) { m, ok := inferenceModules[env]; return m, ok }

func AllowedModule(name string) bool {
	if name == "backup-s3" {
		return true
	}
	for _, m := range APIBasedModules {
		if name == m {
			return true
		}
	}
	for _, m := range inferenceModules {
		if name == m {
			return true
		}
	}
	return false
}
