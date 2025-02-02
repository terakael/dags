import base64
import json
import time
import os
from typing import Dict, List
from google import genai
from google.genai import types
from pydantic import BaseModel
from openai import OpenAI

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.models.param import Param


with DAG(
    dag_id="generate_book",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={"story_description": Param(type="string")},
) as dag:

    @task
    def generate_story(dag_run):
        prompt = f"""
        Write a short story aimed at toddlers, using the following description:
        
        ```
        {dag_run.conf['story_description']}
        ```
        
        Make sure the story contains a life lesson, and a humorous twist.  Keep the story positive.  Use simple words.
        
        The output should be eight paragraphs long.
        
        Your output should be a JSON object containing an array of eight paragraphs.
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema={"type": "array", "items": {"type": "string"}},
            ),
        )

        out = json.loads(response.text)
        return out

    @task
    def understand_story(story) -> str:
        prompt = f"""
        Read the following children's story and then:

        1. Provide a concise summary of the story in 1-2 sentences.
        2. Identify the overall theme or central message of the story in a single sentence.

        Story:
        ```
        {json.dumps(story, indent=2)}
        ```
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class StoryInfo(BaseModel):
            summary: str
            theme: str

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json", response_schema=StoryInfo
            ),
        )

        response_json = json.loads(response.text)

        return response_json

    @task
    def get_character_descriptions(story) -> List[Dict]:
        prompt = f"""
        Please read the following children's story and then:
        1. Identify all the main characters in the story.  Characters don't have to be animate; any recurring item or creature is a character.
        2. For each character, create a detailed visual description that can be used to consistently represent them in images.
           Describe in detail all features of the character - do not overlook minor features.
           Do not use generic terms: make sure every feature is explicitly described and detailed.
           Fill in the blanks on your own if features are "unspecified" or "unknown".  Be as descriptive as possible.
           Do not use generic terms such as "young" or "old"; be explicit in age.
        3. Focus on physical appearance, key attributes, and any distinguishing features mentioned or implied in the story.
        4. Do not describe any expressions, or postures.  We want a description that can be used as a base, which can be built upon throughout the story.

        Story:
        ```
        {json.dumps(story, indent=2)}
        ```
        
        Output the results as key/value pairs of character names and descriptions.
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class CharacterDescription(BaseModel):
            character_name: str
            character_description: str

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=list[CharacterDescription],
            ),
        )

        character_descriptions = json.loads(response.text)
        return character_descriptions

    @task
    def get_paragraph_description(paragraph, summary):
        prompt = f"""Provide a scene description suitable for generating an image, focusing on the actions, expressions, and postures of the characters. Describe the characters' actions and postures to convey their emotions, rather than explicitly stating the emotions.  The character descriptions will be provided separately, so make sure to refer to the characters by name. 

        Use the following scene as context:
        ```
        {paragraph}
        ```

        This scene is part of the following story summary:
        ```
        {json.dumps(summary, indent=2)}
        ```

        The output should be a JSON object with two keys:
        ```json
        {{
            "focus": "a description of the overall scene; environment.",
            "action": "a description of the characters' actions and postures."
        }}
        ```
        """

        conn = BaseHook.get_connection("gemini_api")
        client = genai.Client(api_key=conn.password)

        class ParagraphDescription(BaseModel):
            focus: str
            action: str

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ParagraphDescription,
            ),
        )

        paragraph_description = json.loads(response.text)
        return paragraph_description

    story = generate_story()
    summary = understand_story(story)
    character_descriptions = get_character_descriptions(story)

    get_paragraph_description.partial(summary=summary).expand(paragraph=story)
