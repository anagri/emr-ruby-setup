require File.expand_path(File.dirname(__FILE__) + '/../lib/emr_cluster')

describe EMRCluster do
  before(:each) do
    @emr_cluster = EMRCluster.new('emr_cluster_test-instance', true)
  end

  xit 'should launch keep alive cluster' do
    @emr_job_flow = @emr_cluster.launch(false)
    @emr_job_flow.state.should == 'WAITING'
  end

  it 'should launch cluster with hive setup' do
    @emr_job_flow = @emr_cluster.with_hive.launch
    @emr_job_flow.step_details.map {|s| s[:execution_status_detail][:state]}.should == ['COMPLETED', 'COMPLETED']
  end

  xit 'should launch hive step' do
    @emr_job_flow = @emr_cluster.with_hive.hive_job('test-hive-job', 's3://sprinklr/ruby-sdk/hql/hsql-sample.q').launch
    @emr_job_flow
  end

  after(:each) do
    @emr_cluster.terminate
  end
end