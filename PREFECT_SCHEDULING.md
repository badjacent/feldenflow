# Prefect Scheduling Guide for Feldenflow

This guide explains how to schedule and manage the various Prefect flows in the Feldenflow project.

## Flows Overview

The project contains the following Prefect flows across three Python scripts:

1. **fmtt_scraper.py**
   - `fmtt_video_scraper`: Scrapes videos from Feldenkrais Manhattan Training website

2. **youtube_metadata_scraper.py**
   - `youtube_pipeline`: Main flow for YouTube data pipeline
   - `process_channel`: Process videos from a YouTube channel

3. **multi_source_transcription_flow.py**
   - `create_groq_batch_flow`: Create a batch of audio chunks for Groq processing
   - `submit_groq_batch_flow`: Submit a batch of audio chunks to Groq
   - `retrieve_groq_job_status_flow`: Retrieve a batch of audio chunks from Groq
   - `combine_groq_chunks_via_anthropic`: Combine transcription chunks with Anthropic

## Setting Up Prefect Scheduling

The project includes two utility scripts for managing Prefect:

1. **prefect_deployments.py**: Defines all flow deployments with their schedules
2. **prefect_setup.py**: Utility script for managing the Prefect server and deployments

### Initial Setup

To set up Prefect with all your flows scheduled using an external Prefect server:

```bash
# Make the setup script executable
chmod +x scripts/prefect_setup.py

# Run the complete setup (connects to server, creates pool, applies deployments, starts agent)
python scripts/prefect_setup.py setup --api-url http://your-server-address:4200/api
```

This will:
1. Connect to your existing Prefect server (defaults to http://localhost:4200/api if --api-url is not provided)
2. Create a work pool called "feldenflow-pool"
3. Apply all flow deployments with their schedules
4. Start an agent to execute the scheduled flows

### Current Schedule Configuration

#### Standard Schedules (Time-based)

| Flow | Schedule | Description |
|------|----------|-------------|
| fmtt_video_scraper | Weekly (Sunday 2 AM) | Scrapes FMTT videos weekly |
| youtube_pipeline | Daily (3 AM) | Updates YouTube metadata daily |

#### Process-Until-Empty Flows (Work-based)

For the transcription pipeline, we use a "process-until-empty" pattern, where each flow:
1. Checks if there's work to do
2. Processes all available work until the queue is empty
3. Stops when there's no more work
4. Wakes up again after the scheduled interval

| Flow | Schedule | Description |
|------|----------|-------------|
| process_unprocessed_audio | Every 30 minutes | Creates batches for all unprocessed audio files |
| process_pending_batches | Every 30 minutes | Submits all pending batches to Groq |
| process_submitted_batches | Every 30 minutes | Checks status for all submitted Groq jobs |
| process_completed_batches | Every 30 minutes | Combines all completed chunks into final transcriptions |

This approach is more efficient than the previous time-based scheduling because:
- Flows only run when there's actual work to do
- Each flow processes all available work in one run
- Prevents work from backing up in the queue

### Individual Commands

You can also run specific actions separately:

```bash
# Connect to an existing Prefect server
python scripts/prefect_setup.py connect-server --api-url http://your-server-address:4200/api

# Create a work pool (with custom name if needed)
python scripts/prefect_setup.py create-pool --pool-name my-custom-pool

# Apply all deployments defined in prefect_deployments.py
python scripts/prefect_setup.py apply-deployments

# Start an agent for a specific work pool
python scripts/prefect_setup.py start-agent --pool-name feldenflow-pool

# List all deployments
python scripts/prefect_setup.py list-deployments

# List all flow runs
python scripts/prefect_setup.py list-flows

# Delete all deployments (USE WITH CAUTION)
python scripts/prefect_setup.py delete-all
```

## Modifying Schedules

To change the scheduling of flows:

1. Edit the `prefect_deployments.py` file
2. Update the schedule parameters for the desired flows
3. Apply the updated deployments:
   ```bash
   python scripts/prefect_setup.py apply-deployments
   ```

## Viewing the Prefect UI

The Prefect UI is available at http://localhost:4200 when the server is running.

You can use it to:
- Monitor flow runs
- Trigger manual flow runs
- View logs and flow run history
- Manage work pools and workers

## Troubleshooting

If you encounter issues:

1. Ensure all environment variables are properly set
2. Check if the Prefect server is running (`prefect server status`)
3. Verify that an agent is running for your work pool
4. Check logs in the Prefect UI

For more information, refer to the [Prefect documentation](https://docs.prefect.io/).