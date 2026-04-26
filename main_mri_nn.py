import streamlit as st
import torch
from PIL import Image
import torchvision.transforms as transforms

from model import BrainTumorDenseNet

MODEL_PATH = "MRI_DenseNet121_99%ACC.pth"
CLASSES = ['glioma_tumor', 'meningioma_tumor', 'no_tumor', 'pituitary_tumor']

CLASS_TO_RU = {
    'glioma_tumor': 'Глиома',
    'meningioma_tumor': 'Менингиома',
    'no_tumor': 'Опухоль не обнаружена',
    'pituitary_tumor': 'Гипофизарная опухоль'
}

st.markdown("""
    <style>
    .main {
        text-align: center;
    }

    h1, h2, h3 {
        text-align: center;
    }

    .stFileUploader {
        margin: auto;
    }
    </style>
""", unsafe_allow_html=True)

st.set_page_config(
    page_icon="🧠"
)

# --- LOAD MODEL ---
@st.cache_resource
def load_model():
    model = BrainTumorDenseNet(num_classes=len(CLASSES))

    state_dict = torch.load(MODEL_PATH, map_location="cpu")
    model.load_state_dict(state_dict)

    model.eval()
    return model

model = load_model()

# --- ТРАНСФОРМЫ (КРИТИЧНО) ---
transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize(   # почти наверняка у тебя было это
        mean=[0.485, 0.456, 0.406],
        std=[0.229, 0.224, 0.225]
    )
])

# --- UI ---
st.title("🧠 Нейросеть для классификации опухолей мозга")


uploaded_file = st.file_uploader("Upload MRI", type=["jpg", "png", "jpeg"])

if uploaded_file:
    image = Image.open(uploaded_file).convert("RGB")
    st.image(image, use_column_width=True)

    input_tensor = transform(image).unsqueeze(0)

    with torch.no_grad():
        output = model(input_tensor)
        probs = torch.softmax(output, dim=1)[0]

    pred_idx = torch.argmax(probs).item()
    pred_class = CLASSES[pred_idx]

    st.subheader(f"🧾 Предсказание: {CLASS_TO_RU[pred_class]}")
    st.write(f"🎯 Уверенность: {probs[pred_idx].item():.2%}")

    st.write("### 📊 Вероятности:")

    # подписи по-русски для графика
    probs_np = probs.numpy()

    st.bar_chart(
        {
            CLASS_TO_RU[CLASSES[i]]: float(probs_np[i])
            for i in range(len(CLASSES))
        }
    )