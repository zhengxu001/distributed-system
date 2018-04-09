require 'json'
class Membership
  attr_accessor :master, :slave, :all_nodes, :total_number, :node_num

  def initialize(group_name, master, slave = [])
    @master = master
    @slave = slave
    @all_nodes = @slave + [@master]
    @total_number = @all_nodes.uniq.size
    @group_name = group_name
    update_group_list
  end

  def slave_nodes
    nodes=[]
    @slave.each do |node|
      nodes << node["port"]
    end
  end

  def update_group_list
    begin
      groups = JSON.parse File.read("group_list")
    rescue
      groups = []
    end
    groups.delete_if { |group| group["group_name"] == @group_name}
    groups << self.to_json
    file = File.open("group_list", "w")
    file.write JSON.pretty_generate(groups)
    file.close
  end

  def self.delete_gruop(group_name)
    begin
      groups = JSON.parse File.read("group_list")
    rescue
      groups = []
    end
    groups.delete_if { |group| group["group_name"] == group_name}
    file = File.open("group_list", "w")
    file.write JSON.pretty_generate(groups)
    file.close
  end

  def update
    @all_nodes = @slave + [@master]
    @total_number = @all_nodes.uniq.size
  end

  def to_json
    {
      "master" => @master,
      "slave" => slave,
      "all_nodes" => @all_nodes.uniq,
      "total_number" => @total_number,
      "group_name" => @group_name
    }
  end
end

module GroupList
  
  def self.list
    groups = JSON.parse File.read("group_list")
  rescue
    []
  end

  def self.list_name
    names = []
    groups = JSON.parse File.read("group_list")
    groups.each do |group|
      names << group["group_name"]
    end
    names
  rescue
    []
  end
end