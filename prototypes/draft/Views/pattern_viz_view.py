import os
import pandas as pd
import ast
import streamlit as st
from prototypes.draft.functions import assign_node_positions, change_page
from pyvis.network import Network
import streamlit.components.v1 as components
from collections import defaultdict

'''
    # Load the dataset
    file_path = os.path.join(
        os.path.dirname(__file__), "EMMA_Pattern_Vorschau_JSON.csv"
    )
    df = pd.read_csv(file_path)
    df["episode"] = df["episode"].apply(ast.literal_eval)
'''
def pattern_viz_view():
    st.title("Frequent Episodes Visualization")

    if "episodes" not in st.session_state or not st.session_state.episodes:
        st.warning("No mined episodes found. Please run the mining step first.")
        if st.button("Back"):
            if st.session_state.operation_mode == "uploadCSV":
                change_page("pattern_mining")
        return

    # Convert episodes to DataFrame
    df = pd.DataFrame(st.session_state.episodes)
    # Filter episodes by support range
    sup_min, sup_max = float(df["Support"].min()), float(df["Support"].max())
    support_range = st.slider(
        "Support range",
        min_value=sup_min,
        max_value=sup_max,
        value=(sup_min, sup_max),
    )
    filtered_df = df[df["Support"].between(support_range[0], support_range[1])]

    # Scan which activities are mentioned in each episode
    activities = set()
    for episode in filtered_df["Episode"]:
        for entry in episode:
            activities.update(entry["activity"])
    available_activities = sorted(activities)

    # Let user choose activities that must be in episode
    selected_activities = st.multiselect(
        "Episode must include all of these activities",
        options=available_activities,
    )

    if selected_activities:

        def episode_contains_selected_activities(episode):
            activities_in_episode = {entry["activity"] for entry in episode}
            return all(
                activity in activities_in_episode for activity in selected_activities
            )

        filtered_df = filtered_df[
            filtered_df["Episode"].apply(episode_contains_selected_activities)
        ]

    # If no patterns match, show warning
    if filtered_df.empty:
        st.warning("No patterns match the current filters.")
        return

    # Sort the filtered DataFrame by support in descending order
    filtered_df = filtered_df.sort_values(by="Support", ascending=False)

    # Get patterns
    emma_patterns = filtered_df.to_dict(orient="records")

    # Display patterns in tiles with basic information
    for pattern in emma_patterns:
        pattern_id = pattern["PatternID"]
        support = pattern["Support"]
        with st.expander(f"Pattern ID: {pattern_id} (Support: {support})"):
            # When the expander is clicked, show detailed information
            st.write("### Episode Details")

            # Create network graph
            selected_pattern = pattern["Episode"]
            net = Network(height="800px", width="100%", directed=True)

            # Object-Colors
            all_objs = sorted(
                {obj for step in selected_pattern for obj in step["objects"]}
            )
            colors = ["#114b5f", "#698f3f", "#56203d", "#cc3f0c", "#2f2504"]
            color_map = {obj: colors[i % len(colors)] for i, obj in enumerate(all_objs)}

            # Create activity nodes
            activity_ids = []
            for i, step in enumerate(selected_pattern):
                act_id = f"a{i}"
                activity_ids.append((act_id, step["activity"], step["objects"]))

            # Create object map
            obj_event_map = defaultdict(list)
            for act_id, _, objects in activity_ids:
                for obj in objects:
                    obj_event_map[obj].append(act_id)

            # Calculate Layout
            positions = assign_node_positions(selected_pattern, obj_event_map)

            # Add activity nodes to graph
            for act_id, label, _ in activity_ids:
                x, y = positions[act_id]
                net.add_node(
                    act_id,
                    label=label,
                    shape="box",
                    color="#cccccc",
                    x=x,
                    y=y,
                    physics=False,
                    font={"color": "black"},
                )

            # Add object nodes and edges between activities
            transition_counter = defaultdict(int)
            for obj, events in obj_event_map.items():
                for src, tgt in zip(events, events[1:]):
                    transition_counter[(src, tgt)] += 1

            for obj, events in obj_event_map.items():
                start_id = f"obj_start_{obj}"
                end_id = f"obj_end_{obj}"
                color = color_map[obj]

                x_start, y_start = positions[start_id]
                x_end, y_end = positions[end_id]

                # Add object start and end nodes
                net.add_node(
                    start_id,
                    label=f"    Start: {obj}    ",
                    shape="ellipse",
                    color=color,
                    x=x_start,
                    y=y_start,
                    physics=False,
                    font={"color": "white"},
                    title=f"    {obj}    ",
                )
                net.add_node(
                    end_id,
                    label=f"    End: {obj}    ",
                    shape="ellipse",
                    color=color,
                    x=x_end,
                    y=y_end,
                    physics=False,
                    font={"color": "white"},
                    title=f"    {obj}    ",
                )

                # Add edges
                net.add_edge(start_id, events[0], color=color, width=2, title=obj)
                for src, tgt in zip(events, events[1:]):
                    roundness = 1.0 - (1.0 / transition_counter[(src, tgt)])
                    net.add_edge(
                        src,
                        tgt,
                        color=color,
                        width=2,
                        title=obj,
                        arrows="to",
                        physics=False,
                        smooth={
                            "enabled": True,
                            "type": "curvedCW",
                            "roundness": roundness,
                        },
                    )
                    transition_counter[(src, tgt)] -= 1
                net.add_edge(events[-1], end_id, color=color, width=2, title=obj)

            net.set_options(
                """
                {
                  "physics": { "enabled": false },
                  "interaction": {
                    "dragNodes": true,
                    "dragView": true,
                    "zoomView": true
                  }
                }
                """
            )
            # Generate HTML Code for Network
            html = net.generate_html()

            # Run Fit on all nodes, to adapt Network to Window Size
            html += """
            <script type="text/javascript">
                network.once('afterDrawing', function() {
                    setTimeout(function() {
                        network.fit();
                    }, 50);
                });
            </script>
            """
            components.html(html, height=800)
    if st.button("Back"):
        if st.session_state.operation_mode == "uploadCSV":
            change_page("pattern_mining")
