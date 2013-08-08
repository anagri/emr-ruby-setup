require 'base'

class EMRJobFlow

  def initialize(job_flow)
    @job_flow = job_flow
  end

  def wait_till_ready(timeout = 360)
    total_time = 0
    wait_time = 60
    while total_time <= timeout && @job_flow.state != 'WAITING'
      puts "Sleeping for another #{wait_time} seconds of total #{total_time} seconds for EMR Cluster to be ready. Current state #{@job_flow.state}."
      sleep(wait_time)
      total_time+=wait_time
    end
    throw 'Timeout waiting for EMR cluster' if @job_flow.state != 'WAITING'
    puts "Launched cluster #{@job_flow.id}"
  end

  def state
    @job_flow.state
  end

  def terminate
    @job_flow.terminate
  end
end

class EMRCluster
  def initialize(name='default-emr-cluster', keep_alive=false, config={})
    @emr ||= AWS::EMR.new
    @name = name
    @keep_alive = keep_alive
    @config = {
        'log_uri' => 's3://sprinklr/ruby-sdk/logs/',
        'instance_count' => 2,
        'master_instance_type' => 'm1.small',
        'slave_instance_type' => 'm1.small'
    }.merge(config)
  end

  def launch(async = false)
    puts 'Launching EMR Cluster ...'

    job_flow = @emr.job_flows.create(@name, {
        :log_uri => @config['log_uri'],
        :instances => {
            :instance_count => @config['instance_count'],
            :master_instance_type => @config['master_instance_type'],
            :slave_instance_type => @config['slave_instance_type'],
            :keep_job_flow_alive_when_no_steps => @keep_alive,
            :ec2_key_name => 'emr'
        }
    })

    emr_job_flow = EMRJobFlow.new(job_flow)
    return emr_job_flow if async

    emr_job_flow.wait_till_ready
    emr_job_flow
  end

  def launch_hive
    @emr.job_flows.create(
        'emr-hive-sample',
        {
            :log_uri => 's3://sprinklr/ruby-sdk/logs/',
            :instances => {
                :instance_count => 2,
                :master_instance_type => 'm1.small',
                :slave_instance_type => 'm1.small',
                :keep_job_flow_alive_when_no_steps => true,
                :ec2_key_name => 'emr'
            },
            :steps => [
                {
                    :name => 'emr-hive-setup',
                    :action_on_failure => 'TERMINATE_JOB_FLOW',
                    :hadoop_jar_step => {
                        :jar => "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                        :args => [
                            "s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                            "--base-path", "s3://us-east-1.elasticmapreduce/libs/hive/",
                            "--install-hive",
                            "--hive-versions", "latest"
                        ]
                    }
                },
                {
                    :name => 'emr-hive-site-setup',
                    :action_on_failure => 'TERMINATE_JOB_FLOW',
                    :hadoop_jar_step => {
                        :jar => "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                        :args => [
                            "s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                            "--base-path", "s3://us-east-1.elasticmapreduce/libs/hive/",
                            "--install-hive-site", "--hive-site", "s3://sprinklr/conf/hive/hive-site.xml",
                            "--hive-versions", "latest"
                        ]
                    }
                },
                {
                    :name => 'show-hive-tables',
                    :action_on_failure => 'CONTINUE',
                    :hadoop_jar_step => {
                        :jar => "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                        :args => ["s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                                  "--base-path", "s3://us-east-1.elasticmapreduce/libs/hive/",
                                  "--hive-versions", "latest",
                                  "--run-hive-script", "--args", "-f", "s3://sprinklr/ruby-sdk/hql/hsql-sample.q"]
                    }
                }]
        })
  end
end

