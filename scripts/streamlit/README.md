# Streamlit App

## Getting started

1.  In a terminal run:

    ```bash
    pyenv shell streamlit
    streamlit run solution/streamlit/example.py
    ```

    The example streamlit app should startup and display a preview.

## Auto starting in DevReady

In the event that you want a streamlit app to startup when you first open
DevReady, all you need to do is update the following:

1.  Open `dataops-dde/post-setup.sh` which runs at the end of the startup setup.
1.  Add the following commands:

    ```bash
    pyenv shell streamlit
    streamlit run solution/streamlit/example.py
    ```

    > **NOTE:** Only 1 streamlit app can run this way.