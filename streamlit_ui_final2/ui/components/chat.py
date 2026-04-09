import streamlit as st
from ui.runtime import request_rerun


def chat_form_split(title: str, default_prompt: str, form_renderer, context_key: str):
    st.subheader(title)
    split_ratio = st.slider("Resize panels", 20, 80, 40, help="Adjust NL panel vs form panel.",
                            key=f"{context_key}__resize")
    col_chat, col_form = st.columns([split_ratio, 100 - split_ratio])

    with col_chat:
        st.markdown("**🧠 Natural Language**")
        for msg in st.session_state.chat_history[-20:]:
            st.markdown(f"**{msg['role']}**: {msg['text']}")
        draft_key = f"{context_key}__draft"
        current_draft = st.session_state.chat_draft_by_context.get(draft_key, "")
        chat_input = st.text_area("NL Input", value=current_draft, height=120, key=f"{context_key}__chat_input")
        if st.button("Send NL", type="primary", key=f"{context_key}__send_nl"):
            if chat_input.strip():
                st.session_state.chat_history.append({"role": "User", "text": chat_input})
                st.session_state.chat_history.append(
                    {"role": "Assistant", "text": "✅ Parsed intent and suggested values (mock)."})
                st.session_state.chat_draft_by_context[draft_key] = ""
                request_rerun()
            else:
                st.warning("Type something before sending.")
        if not st.session_state.chat_history:
            st.info(default_prompt)
        st.session_state.chat_draft_by_context[draft_key] = chat_input

    with col_form:
        st.markdown("**📝 Configuration Form (Final Authority)**")
        form_renderer()
