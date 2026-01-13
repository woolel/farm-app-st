import streamlit as st
print(f"Streamlit Version: {st.__version__}")
print(f"Has dialog: {hasattr(st, 'dialog')}")
print(f"Has experimental_dialog: {hasattr(st, 'experimental_dialog')}")
print(f"Has popover: {hasattr(st, 'popover')}")
