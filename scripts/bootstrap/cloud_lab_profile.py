"""Nova
"""

# Import the Portal object.
import geni.portal as portal
# Import the ProtoGENI library.
import geni.rspec.pg as pg
# Import the Emulab specific extensions.
import geni.rspec.emulab as emulab

# Create a portal object,
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

pc.defineParameter("node_type", "Hardware spec of nodes to use. <br> Refer <a href=\"http://docs.aptlab.net/hardware.html#%28part._apt-cluster%29\">manual</a> for more details.",
 portal.ParameterType.NODETYPE, "c6220", legalValues=["r320", "c6220"], advanced=False, groupId=None)
pc.defineParameter("num_nodes", "Number of nodes to use.<br> Check cluster availability <a href=\"https://www.cloudlab.us/cluster-graphs.php\">here</a>.",
 portal.ParameterType.INTEGER, 2, legalValues=[], advanced=False, groupId=None)

DISK_IMAGE = 'urn:publicid:IDN+emulab.net+image+emulab-ops//UBUNTU16-64-STD'

params = pc.bindParameters()
if params.num_nodes < 1:
    pc.reportError(portal.ParameterError("You must choose a minimum of 1 node "))
pc.verifyParameters()

# Link link-0
link = request.Link('link-0')
link.Site('site-1')

for i in range(params.num_nodes):
    node = request.RawPC("node-%s" % i)
    node.hardware_type = params.node_type
    node.disk_image = DISK_IMAGE
    node.routable_control_ip = True
    node.Site("site-1")
    iface = node.addInterface("interface-%s" % i)
    ip = "10.0.1." + str(i+1)
    iface.addAddress(pg.IPv4Address(ip, "255.255.255.0"))
    link.addInterface(iface)

# Print the generated rspec
pc.printRequestRSpec(request)
