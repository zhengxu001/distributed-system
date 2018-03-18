class Membership
  attr_accessor :master, :slave, :version, :all_nodes, :total_number

  def initialize(master, slave = [])
    @master = master
    @slave = slave
    @all_nodes = slave << master
    @total_number = @all_nodes.uniq.size
  end

  def to_json
    {
      master: @master,
      slave: @slave.uniq,
      all_nodes: @all_nodes.uniq,
      total_number: @total_number
    }.to_json
  end

  def has_majority?(count)
    count >= (@total_number / 2) + 1
  end
end