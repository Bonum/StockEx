#!/usr/bin/env python3
"""Convert the StockEx CH Trader LoRA adapter to GGUF for Ollama.

Prerequisites:
    pip install torch transformers peft huggingface_hub
    git clone https://github.com/ggerganov/llama.cpp
    cd llama.cpp && pip install -r requirements/requirements-convert_hf_to_gguf.txt

Usage:
    python scripts/convert_to_ollama.py

This script will:
    1. Download the base model (Qwen2.5-7B-Instruct)
    2. Download the LoRA adapter (RayMelius/stockex-ch-trader)
    3. Merge adapter into base model (CPU, ~16GB RAM needed)
    4. Convert merged model to GGUF (Q4_K_M quantization)
    5. Create and register an Ollama model

After running, use in StockEx with:
    OLLAMA_HOST=http://localhost:11434  OLLAMA_MODEL=stockex-ch-trader
"""

import os
import sys
import shutil
import subprocess
import argparse

BASE_MODEL = "Qwen/Qwen2.5-7B-Instruct"
ADAPTER_REPO = "RayMelius/stockex-ch-trader"
OLLAMA_MODEL_NAME = "stockex-ch-trader"
QUANT = "Q4_K_M"

WORK_DIR = os.path.join(os.path.dirname(__file__), "..", "models")
MERGED_DIR = os.path.join(WORK_DIR, "merged")
GGUF_PATH = os.path.join(WORK_DIR, f"stockex-ch-trader-{QUANT}.gguf")
MODELFILE_PATH = os.path.join(WORK_DIR, "Modelfile")

SYSTEM_PROMPT = (
    "You are a StockEx clearing house trading agent. "
    "Given a member's financial state and live market data, "
    "you output a single valid JSON trading decision that respects all capital and holdings constraints. "
    "Never output anything other than the JSON object."
)


def step(n, msg):
    print(f"\n{'='*60}")
    print(f"  Step {n}: {msg}")
    print(f"{'='*60}\n")


def merge_adapter():
    """Download base model + adapter, merge, save to disk."""
    step(1, f"Merging {ADAPTER_REPO} into {BASE_MODEL}")

    import torch
    from transformers import AutoTokenizer, AutoModelForCausalLM
    from peft import PeftModel

    print(f"Loading base model (CPU, float16)...")
    base_model = AutoModelForCausalLM.from_pretrained(
        BASE_MODEL,
        torch_dtype=torch.float16,
        device_map="cpu",
        trust_remote_code=True,
    )

    print(f"Loading adapter from {ADAPTER_REPO}...")
    model = PeftModel.from_pretrained(base_model, ADAPTER_REPO)

    print("Merging adapter weights...")
    model = model.merge_and_unload()

    os.makedirs(MERGED_DIR, exist_ok=True)
    print(f"Saving merged model to {MERGED_DIR}...")
    model.save_pretrained(MERGED_DIR)

    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL, trust_remote_code=True)
    tokenizer.save_pretrained(MERGED_DIR)
    print("Merge complete.")


def convert_to_gguf(llama_cpp_dir):
    """Convert merged HF model to GGUF format."""
    step(2, f"Converting to GGUF ({QUANT})")

    convert_script = os.path.join(llama_cpp_dir, "convert_hf_to_gguf.py")
    if not os.path.exists(convert_script):
        print(f"ERROR: {convert_script} not found.")
        print(f"Clone llama.cpp first: git clone https://github.com/ggerganov/llama.cpp")
        sys.exit(1)

    # First convert to f16 GGUF
    f16_path = os.path.join(WORK_DIR, "stockex-ch-trader-f16.gguf")
    cmd = [sys.executable, convert_script, MERGED_DIR, "--outfile", f16_path, "--outtype", "f16"]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    # Then quantize
    quantize_bin = os.path.join(llama_cpp_dir, "build", "bin", "llama-quantize")
    if not os.path.exists(quantize_bin):
        # Try alternative paths
        for alt in ["llama-quantize", "quantize"]:
            alt_path = os.path.join(llama_cpp_dir, "build", "bin", alt)
            if os.path.exists(alt_path):
                quantize_bin = alt_path
                break
            # Check if it's in PATH
            if shutil.which(alt):
                quantize_bin = alt
                break

    if os.path.exists(quantize_bin) or shutil.which(quantize_bin):
        cmd = [quantize_bin, f16_path, GGUF_PATH, QUANT]
        print(f"Quantizing: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        os.remove(f16_path)
        print(f"Quantized GGUF saved to {GGUF_PATH}")
    else:
        # No quantize binary — keep f16
        os.rename(f16_path, GGUF_PATH)
        print(f"llama-quantize not found, using f16 GGUF: {GGUF_PATH}")
        print(f"To quantize manually: llama-quantize {GGUF_PATH} output.gguf {QUANT}")


def create_ollama_model():
    """Create Ollama Modelfile and register the model."""
    step(3, "Creating Ollama model")

    gguf_abs = os.path.abspath(GGUF_PATH)

    modelfile_content = f"""FROM {gguf_abs}

SYSTEM \"\"\"{SYSTEM_PROMPT}\"\"\"

PARAMETER temperature 0.4
PARAMETER num_predict 100
PARAMETER stop "<|im_end|>"
PARAMETER stop "<|endoftext|>"
"""

    with open(MODELFILE_PATH, "w") as f:
        f.write(modelfile_content)
    print(f"Modelfile written to {MODELFILE_PATH}")

    # Check if Ollama is available
    if not shutil.which("ollama"):
        print("\nOllama not found in PATH. Install from https://ollama.com")
        print(f"Then run manually:")
        print(f"  ollama create {OLLAMA_MODEL_NAME} -f {os.path.abspath(MODELFILE_PATH)}")
        return

    cmd = ["ollama", "create", OLLAMA_MODEL_NAME, "-f", MODELFILE_PATH]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Ollama model '{OLLAMA_MODEL_NAME}' created successfully!")
        print(f"\nTest it:")
        print(f"  ollama run {OLLAMA_MODEL_NAME}")
        print(f"\nUse in StockEx docker-compose.yml:")
        print(f"  OLLAMA_HOST=http://host.docker.internal:11434")
        print(f"  OLLAMA_MODEL={OLLAMA_MODEL_NAME}")
    else:
        print(f"Ollama create failed: {result.stderr}")
        print(f"Try manually: ollama create {OLLAMA_MODEL_NAME} -f {os.path.abspath(MODELFILE_PATH)}")


def main():
    parser = argparse.ArgumentParser(description="Convert StockEx CH Trader to Ollama GGUF")
    parser.add_argument("--llama-cpp", default=os.path.expanduser("~/llama.cpp"),
                        help="Path to llama.cpp repo (default: ~/llama.cpp)")
    parser.add_argument("--skip-merge", action="store_true",
                        help="Skip merge step (use existing merged model)")
    parser.add_argument("--skip-convert", action="store_true",
                        help="Skip GGUF conversion (use existing GGUF)")
    args = parser.parse_args()

    os.makedirs(WORK_DIR, exist_ok=True)

    if not args.skip_merge:
        merge_adapter()
    else:
        print(f"Skipping merge (using {MERGED_DIR})")

    if not args.skip_convert:
        convert_to_gguf(args.llama_cpp)
    else:
        print(f"Skipping conversion (using {GGUF_PATH})")

    create_ollama_model()

    print(f"\n{'='*60}")
    print(f"  DONE!")
    print(f"{'='*60}")
    print(f"  Merged model : {MERGED_DIR}")
    print(f"  GGUF file    : {GGUF_PATH}")
    print(f"  Ollama model : {OLLAMA_MODEL_NAME}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
