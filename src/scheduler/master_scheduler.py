# Master Pipeline Scheduler Implementation

# src/scheduler/master_scheduler.py
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import croniter
from sqlalchemy.orm import Session

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"

@dataclass
class PipelineJob:
    name: str
    collector_class: str
    transformer_class: str
    loader_class: str
    schedule: str  # cron expression
    priority: int = 5  # 1=highest, 10=lowest
    timeout_seconds: int = 600
    retry_count: int = 3
    retry_delay: int = 60
    status: JobStatus = JobStatus.PENDING
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    error_count: int = 0
    dependencies: List[str] = field(default_factory=list)

class MasterScheduler:
    def __init__(self):
        self.jobs: Dict[str, PipelineJob] = {}
        self.running_jobs: Dict[str, asyncio.Task] = {}
        self.logger = logging.getLogger(__name__)
        self.shutdown_event = asyncio.Event()
        
        self._register_jobs()
    
    def _register_jobs(self):
        """Register all pipeline jobs with their schedules"""
        
        job_definitions = {
            'crypto': PipelineJob(
                name='crypto',
                collector_class='CryptoCollector',
                transformer_class='CryptoTransformer', 
                loader_class='CryptoLoader',
                schedule='0 */4 * * *',  # Every 4 hours
                priority=1,
                timeout_seconds=600,
                retry_count=3
            ),
            'money_market': PipelineJob(
                name='money_market',
                collector_class='MoneyMarketCollector',
                transformer_class='MoneyMarketTransformer',
                loader_class='MoneyMarketLoader', 
                schedule='0 6 * * *',  # Daily at 6 AM EAT
                priority=2,
                timeout_seconds=1800,
                retry_count=2,
                dependencies=['forex']  # Needs forex rates for conversion
            ),
            'treasury': PipelineJob(
                name='treasury',
                collector_class='TreasuryCollector',
                transformer_class='TreasuryTransformer',
                loader_class='TreasuryLoader',
                schedule='30 9 * * 1-5',  # Daily at 9:30 AM EAT, weekdays only
                priority=1,
                timeout_seconds=1200,
                retry_count=3
            ),
            'forex': PipelineJob(
                name='forex', 
                collector_class='ForexCollector',
                transformer_class='ForexTransformer',
                loader_class='ForexLoader',
                schedule='0 6,12,18 * * 1-5',  # 3x daily weekdays
                priority=2,
                timeout_seconds=900,
                retry_count=2
            ),
            'market_indexes': PipelineJob(
                name='market_indexes',
                collector_class='IndexesCollector', 
                transformer_class='IndexesTransformer',
                loader_class='IndexesLoader',
                schedule='0 */6 * * 1-5',  # Every 6 hours weekdays
                priority=3,
                timeout_seconds=1800,
                retry_count=3
            ),
            'green_bonds': PipelineJob(
                name='green_bonds',
                collector_class='GreenBondsCollector',
                transformer_class='GreenBondsTransformer',
                loader_class='GreenBondsLoader', 
                schedule='0 14 * * 5',  # Weekly Friday 2 PM
                priority=4,
                timeout_seconds=1800,
                retry_count=2
            ),
            'fixed_deposits': PipelineJob(
                name='fixed_deposits',
                collector_class='FixedDepositsCollector',
                transformer_class='FixedDepositsTransformer',
                loader_class='FixedDepositsLoader',
                schedule='0 10 * * 3',  # Weekly Wednesday 10 AM
                priority=4,
                timeout_seconds=1200,
                retry_count=2
            ),
            'carbon_offsets': PipelineJob(
                name='carbon_offsets', 
                collector_class='CarbonOffsetsCollector',
                transformer_class='CarbonOffsetsTransformer',
                loader_class='CarbonOffsetsLoader',
                schedule='0 15 * * 5',  # Weekly Friday 3 PM
                priority=5,
                timeout_seconds=900,
                retry_count=1
            )
        }
        
        for job_name, job in job_definitions.items():
            self._schedule_next_run(job)
            self.jobs[job_name] = job
    
    def _schedule_next_run(self, job: PipelineJob):
        """Calculate next run time based on cron schedule"""
        cron = croniter.croniter(job.schedule, datetime.now())
        job.next_run = cron.get_next(datetime)
    
    async def run_forever(self):
        """Main scheduler loop"""
        self.logger.info("Master scheduler started")
        
        while not self.shutdown_event.is_set():
            try:
                await self._check_and_run_jobs()
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    async def _check_and_run_jobs(self):
        """Check which jobs should run and execute them"""
        now = datetime.now()
        
        # Find jobs that should run
        ready_jobs = []
        for job_name, job in self.jobs.items():
            if (job.next_run <= now and 
                job_name not in self.running_jobs and
                job.status != JobStatus.RUNNING):
                ready_jobs.append(job)
        
        # Sort by priority (1 = highest priority)
        ready_jobs.sort(key=lambda x: x.priority)
        
        # Execute jobs respecting dependencies
        for job in ready_jobs:
            if await self._dependencies_satisfied(job):
                await self._execute_job(job)
    
    async def _dependencies_satisfied(self, job: PipelineJob) -> bool:
        """Check if job dependencies have completed successfully"""
        if not job.dependencies:
            return True
            
        for dep_name in job.dependencies:
            dep_job = self.jobs.get(dep_name)
            if not dep_job or dep_job.status != JobStatus.SUCCESS:
                # Check if dependency ran recently enough
                if (dep_job.last_run and 
                    datetime.now() - dep_job.last_run < timedelta(hours=24)):
                    continue  # Dependency is recent enough
                else:
                    self.logger.info(f"Job {job.name} waiting for dependency {dep_name}")
                    return False
        return True
    
    async def _execute_job(self, job: PipelineJob):
        """Execute a pipeline job with timeout and error handling"""
        job.status = JobStatus.RUNNING
        self.logger.info(f"Starting job: {job.name}")
        
        try:
            # Create task with timeout
            task = asyncio.create_task(self._run_pipeline(job))
            self.running_jobs[job.name] = task
            
            # Wait with timeout
            await asyncio.wait_for(task, timeout=job.timeout_seconds)
            
            # Job completed successfully
            job.status = JobStatus.SUCCESS
            job.last_run = datetime.now()
            job.error_count = 0
            self._schedule_next_run(job)
            
            self.logger.info(f"Job {job.name} completed successfully")
            
        except asyncio.TimeoutError:
            job.status = JobStatus.FAILED
            job.error_count += 1
            self.logger.error(f"Job {job.name} timed out after {job.timeout_seconds}s")
            await self._handle_job_failure(job)
            
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_count += 1
            self.logger.error(f"Job {job.name} failed: {e}")
            await self._handle_job_failure(job)
            
        finally:
            # Clean up
            if job.name in self.running_jobs:
                del self.running_jobs[job.name]
    
    async def _run_pipeline(self, job: PipelineJob):
        """Execute the full ETL pipeline for a job"""
        
        # Dynamic import of pipeline components
        collector_module = __import__(f'src.collectors.{job.name}_collector', 
                                    fromlist=[job.collector_class])
        transformer_module = __import__(f'src.transformers.{job.name}_transformer',
                                      fromlist=[job.transformer_class])
        loader_module = __import__(f'src.loaders.database_loader',
                                 fromlist=['DatabaseLoader'])
        
        collector_class = getattr(collector_module, job.collector_class)
        transformer_class = getattr(transformer_module, job.transformer_class)
        loader_class = getattr(loader_module, 'DatabaseLoader')
        
        # Initialize pipeline components
        collector = collector_class()
        transformer = transformer_class()
        loader = loader_class()
        
        # Execute ETL pipeline
        self.logger.info(f"[{job.name}] Starting data collection")
        raw_data = await collector.collect_data()
        
        self.logger.info(f"[{job.name}] Collected {len(raw_data)} records, starting transformation")
        transformed_data = await transformer.transform(raw_data)
        
        self.logger.info(f"[{job.name}] Transformed {len(transformed_data)} records, starting load")
        load_result = await loader.load(transformed_data, table=job.name)
        
        self.logger.info(f"[{job.name}] Pipeline complete. Loaded {load_result['inserted']} new, updated {load_result['updated']} existing")
        
        return load_result
    
    async def _handle_job_failure(self, job: PipelineJob):
        """Handle job failure with retry logic"""
        if job.error_count <= job.retry_count:
            # Schedule retry
            job.status = JobStatus.RETRYING
            retry_delay = job.retry_delay * (2 ** (job.error_count - 1))  # Exponential backoff
            job.next_run = datetime.now() + timedelta(seconds=retry_delay)
            
            self.logger.info(f"Job {job.name} will retry in {retry_delay}s (attempt {job.error_count}/{job.retry_count})")
            
        else:
            # Max retries exceeded
            job.status = JobStatus.FAILED
            self._schedule_next_run(job)  # Schedule for next regular run
            
            # Send alert
            await self._send_failure_alert(job)
            self.logger.error(f"Job {job.name} failed permanently after {job.retry_count} retries")
    
    async def _send_failure_alert(self, job: PipelineJob):
        """Send alert for permanent job failure"""
        # Implementation depends on your notification preferences (Slack, email, etc.)
        alert_message = f"""
        🚨 Pipeline Job Failed: {job.name}
        
        Status: {job.status.value}
        Error Count: {job.error_count}
        Last Run: {job.last_run}
        Next Scheduled: {job.next_run}
        
        Manual intervention may be required.
        """
        
        # Send to your notification system
        self.logger.critical(alert_message)
    
    def get_status(self) -> Dict:
        """Get current status of all jobs"""
        return {
            job_name: {
                'status': job.status.value,
                'priority': job.priority,
                'last_run': job.last_run,
                'next_run': job.next_run,
                'error_count': job.error_count,
                'is_running': job_name in self.running_jobs
            }
            for job_name, job in self.jobs.items()
        }
    
    async def run_job_now(self, job_name: str) -> bool:
        """Manually trigger a job run"""
        if job_name not in self.jobs:
            return False
            
        job = self.jobs[job_name]
        if job_name in self.running_jobs:
            self.logger.warning(f"Job {job_name} is already running")
            return False
            
        await self._execute_job(job)
        return True
    
    async def shutdown(self):
        """Gracefully shutdown scheduler"""
        self.logger.info("Shutting down scheduler...")
        self.shutdown_event.set()
        
        # Wait for running jobs to complete (with timeout)
        if self.running_jobs:
            self.logger.info(f"Waiting for {len(self.running_jobs)} jobs to complete...")
            running_tasks = list(self.running_jobs.values())
            try:
                await asyncio.wait_for(
                    asyncio.gather(*running_tasks, return_exceptions=True),
                    timeout=300  # 5 minutes max
                )
            except asyncio.TimeoutError:
                self.logger.warning("Some jobs didn't complete within shutdown timeout")

# Usage example
async def main():
    scheduler = MasterScheduler()
    
    # Start scheduler
    scheduler_task = asyncio.create_task(scheduler.run_forever())
    
    try:
        await scheduler_task
    except KeyboardInterrupt:
        await scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(main())