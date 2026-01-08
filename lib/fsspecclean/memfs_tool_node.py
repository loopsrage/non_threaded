from typing import Annotated, TypedDict, NotRequired

from langchain.agents import create_agent
from langchain_ollama import ChatOllama
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import add_messages

from lib.fsspecclean.memfs_toolkit import FSspecToolKit

story = """
In the sleepy town of Willow Creek, where nothing ever happens, lived Elias Thorne, a young man with a mysterious past and eyes the color of a storm-tossed sea.
Elias was a simple clockmaker’s apprentice, but he always felt he was "meant for something more." He spent his days staring out the window of the dusty shop, dreaming of adventures beyond the horizon. He was the classic "chosen one," though he didn’t know it yet, often tripping over his own feet despite possessing a hidden, ancient grace.
One fateful evening, as a dark and stormy night took hold of the town, a hooded stranger arrived at the shop. With a voice like gravel, the stranger delivered a cryptic prophecy: "When the crimson moon rises, the heir of the Sun-King shall reclaim the fractured throne." Elias, naturally, had a birthmark on his shoulder shaped exactly like a sunburst—a mark he had always been told was just a "weird mole."
Suddenly, the shop was attacked by shadowy figures in black capes. Elias discovered that when he held a common fireplace poker, it glowed with a celestial blue light. He fought with an instinctive skill he never knew he possessed, defeating the guards while barely breaking a sweat.
He was forced to flee his home, joined by a ragtag group of misfits:
Kaelen, the cynical rogue with a heart of gold who claimed he was "only in it for the money."
Lyra, the feisty princess who had run away from an arranged marriage and was "not your typical damsel in distress."
Barnaby, the bumbling but loyal comic relief who constantly talked about his mother's cooking.
Their journey took them through the Whispering Woods (which actually whispered) and across the Mountains of Despair. Along the way, Elias and Lyra engaged in constant "bickering that was clearly sexual tension." At one point, there was only one bed at the inn, forcing them to share, though they both insisted they hated the idea.
They eventually reached the Dark Lord’s fortress, a jagged spire of obsidian where it was always lightning but never raining. The Dark Lord, Malakor the Maleficent, revealed the ultimate twist: "Elias, I am your father’s brother’s former roommate... and also, I killed your parents."
In a final climactic battle, Elias was disarmed and cornered. But just as Malakor prepared the killing blow, Elias remembered the "power of friendship" and the words of his dead mentor (who appeared as a glowing blue ghost). He unleashed a literal blast of light from his chest, disintegrating the darkness.
With the villain defeated, the sun broke through the clouds for the first time in a thousand years. Elias was crowned King, Lyra became his Queen (after a dramatic "shut up and kiss me" moment), and they lived happily ever after—until a post-credits scene showed Malakor’s hand twitching in the rubble, hinting at a sequel."""


class AgentRequestIdCsvData(TypedDict):
    request_id: str
    csv_data: NotRequired[str]
    messages: Annotated[list, add_messages]


def fs_react_agent(model, fs):
    return create_agent(
        model=model,
        tools=FSspecToolKit(fs=fs).get_tools(),
        checkpointer=MemorySaver(),
        state_schema=AgentRequestIdCsvData,
        system_prompt="""
You are a strict data assistant.

### CORE GOVERNANCE RULES:
1. The 'request_id' is managed by the system. DO NOT guess, generate, or ask the user for it.
2. The system will automatically inject the correct 'request_id' into tool calls.
3. If 'csv_data' is missing from the context, YOU ARE FORBIDDEN from performing any save operations. 
   Instead, inform the user that data is missing.

### TOOL USAGE:
- Always confirm tool results before finalizing your response.
"""
        )
