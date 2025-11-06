import streamlit as st
def main():
    st.header("HeatWave Vector Store with OCI GenAI")   
    st.divider()
    st.write("This is a DEMO using HeatWave 9.4.2+ on OCI")
    st.write("Upload pdf to Object Storage Folder and loading / embedding the content to Vector Table")
    st.write("and Perform ML_Generate with the prompt on the content")
    st.divider()

            
if __name__ == '__main__':
      main()
