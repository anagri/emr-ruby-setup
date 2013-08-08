require File.expand_path(File.dirname(__FILE__) + '/../lib/emr_cluster')

describe EMRCluster do
  before(:each) do
    @emr_cluster = EMRCluster.new('emr_cluster_test-instance', true)
  end

  it 'should launch keep alive cluster' do
    @emr_job_flow = @emr_cluster.launch(false)
    @emr_job_flow.state.should == 'WAITING'
  end

  after(:each) do
    @emr_job_flow.terminate
  end
end