require 'haversine'
require 'tree'
require 'csv'
require 'time'


# Builds the weather relation hash from weather relation data.
# Used when storm starts are not being found, but the hash needs to be loaded from previous data
def buildWeatherRelationHash ()
	puts "Bulidng weather relation hash from weather relation file"
	$weatherRelationHash = Hash.new
	justWeather = File.open("WeatherRelationData.csv", "r").each do |line|
		temp = line.delete("\n").split(",")
		$weatherRelationHash[temp[0]] = temp.join(",")
	end
end

# Returns the latitude of the given airport code
def getLatAndLongHash()
	latLongHash = Hash.new {""}
	inputData = File.open("Airport_Location.csv", "r").each do |line|
		temp = line.split(",")
		latLongHash[temp[0]] = temp[1]
	end
	inputData.close()
	return latLongHash
end

# Read the bulk traffic file and find the lines that are weather events
def stripTrafficFile()
	puts "Stripping traffic file"
	counter = 0
	airportHash = Hash.new(0)
	latLongHash = getLatAndLongHash()
	justWeather = File.open("WeatherData.csv", "w")
	inputData = File.open("TrafficWeatherEvent_Aug16_Aug18_Publish.csv", "r").each do |line|

		# This RegEx only matches weather events from the traffic data
		if line =~ /\A[W]/

			# Avoides lines that have N/A in data
			next if line =~ /N\/A/

			array = line.split(",")
			break if counter >= 10000

			latAndLong = latLongHash[array[10]].split(" ")
			formattedLine = array[0].to_s + "," + array[1].to_s + "," + array[2].to_s + "," + array[3].to_s + "," + array[5].to_s + "," + array[6].to_s + "," + latAndLong[0].to_s + "," + latAndLong[1].to_s + "," + array[10].to_s + "\n"
			justWeather.write(formattedLine)

			counter = counter + 1
		end
	end
	justWeather.close()
	puts "There were " + counter.to_s  + " weather events identified"
end


# Returns an array of weather events that should be the first event in a storm 
def findStormStarts(timeRange, distanceRange)
	puts "Finding storm starts"

	latLongHash = getLatAndLongHash()
	$weatherRelationHash = Hash.new
	weatherRelation = File.open("WeatherRelationData.csv", "w")

	# Look at every weather event and see if any could be the child of another event
	checkFile = File.open("WeatherDataSorted.csv", "r").each do |check|

		checkTemp = check.delete("\n").split(",")
		foundParent = false

		# Parent storm must have started up to 4 hours before the current storm we look at started
		# This is to decrease the possible range of events needed to check against
		endRange = Time.parse(checkTemp[4])
		startRange = endRange - timeRange
		searchRange = (startRange .. endRange) 


		# Compare the staring event to others. If a possible parent exits, break
		compareFile = File.open("WeatherDataSorted.csv", "r").each do |compare|
			
			compareTemp = compare.delete("\n").split(",")
			# Check if they have the same eventID
			next if checkTemp[0].eql? (compareTemp[0])

			# If the possible parent is not in the range
			if !(searchRange.cover? (Time.parse(compareTemp[4])))
				# If looping hasn't gotten to the range yet, go to the next loop
				# Else we've passed the range and can break
				if Time.parse(compareTemp[4]) < startRange
					next
				else Time.parse(compareTemp[4]) > endRange
					break
				end
			end

			# Compare row is first to see if check could be a parent
			# Check is the parent and goes first
			if heuristic(compareTemp, checkTemp, distanceRange) == 0
				weatherRelation.write(checkTemp[0].to_s + "," + checkTemp[1].to_s + "," + checkTemp[2].to_s + "," + checkTemp[3].to_s + "," + checkTemp[4].to_s + "," + checkTemp[5].to_s + "," + checkTemp[6].to_s + "," + checkTemp[7].to_s + "," + checkTemp[8].to_s + "," + compareTemp[0].to_s + "\n")
				$weatherRelationHash[checkTemp[0]] = checkTemp[0].to_s + "," + checkTemp[1].to_s + "," + checkTemp[2].to_s + "," + checkTemp[3].to_s + "," + checkTemp[4].to_s + "," + checkTemp[5].to_s + "," + checkTemp[6].to_s + "," + checkTemp[7].to_s + "," + checkTemp[8].to_s + "," + compareTemp[0].to_s
				foundParent = true
				break 
			end
		end

		if foundParent == false
			weatherRelation.write(checkTemp[0].to_s + "," + checkTemp[1].to_s + "," + checkTemp[2].to_s + "," + checkTemp[3].to_s + "," + checkTemp[4].to_s + "," + checkTemp[5].to_s + "," + checkTemp[6].to_s + "," + checkTemp[7].to_s + "," + checkTemp[8].to_s + ",~\n")
			$weatherRelationHash[checkTemp[0]] = checkTemp[0].to_s + "," + checkTemp[1].to_s + "," + checkTemp[2].to_s + "," + checkTemp[3].to_s + "," + checkTemp[4].to_s + "," + checkTemp[5].to_s + "," + checkTemp[6].to_s + "," + checkTemp[7].to_s + "," + checkTemp[8].to_s + ",~"
		end

	end
	weatherRelation.close()
end

# Start the tree
def buildStormStart(timeRange, distanceRange)
	puts "Building storm starts"
	fileName = "StormsOutput-" + timeRange.to_s + "-" + distanceRange.to_s + ".txt"
	File.delete(fileName) if File.exist? (fileName)

	$weatherRelationHash.each do |key, value|

		# This RegEx only matches weather events that have a ~.
		# The ~ only apears in the has parent column
		if value =~ /.*~/
			temp = value.split(",")
			# Remove the parent part of the information
			temp.pop
			relatedEvents = Hash.new(0)
			relatedEvents[temp[0].to_s] = 1

			# The root to start the tree
			rootNode = Tree::TreeNode.new(temp[0].to_s, temp.join(","))

			# Builds the tree fully and prints
			buildStormRecursive(rootNode, relatedEvents)
			outputFile = File.open(fileName, "a")
			outputFile.write("Storm " + rootNode.name.to_s + "\n")
			outputFile.close
			printStorm(rootNode, fileName)
			
			# Condenses the tree and prints
			vertical = verticalCondense(rootNode)
			horizontal = horizontalCondense(rootNode)
			if vertical || horizontal
				outputFile = File.open(fileName, "a")
				outputFile.write("Storm " + rootNode.name.to_s + " after being condensed\n")
				outputFile.close
				printStorm(rootNode, fileName)
			end
		end
	end
end

# Builds the tree
def buildStormRecursive(currentNode, relatedEvents)

	current = currentNode.content
	currentTemp = current.split(",")

	# Finds all new events related to the current root
	newEvents = findRelatedEvents(currentTemp[0], relatedEvents)

	if newEvents.length > 0
		# Loop through the first set of children to build the tree
		newEvents.each_with_index do |newEvent, index|
			newTemp = newEvent.split(",")
			# Remove the parent part of the information
			newTemp.pop
			nextNode = Tree::TreeNode.new(newTemp[0].to_s, newTemp.join(","))
			# Add that children's children to them
			buildStormRecursive(nextNode, relatedEvents)
			currentNode << nextNode
		end
	end
end

# Finds the children to a given event
def findRelatedEvents(eventId, relatedEvents)
	newEvents = []

	$weatherRelationHash.each do |key, value|
		temp = value.split(",")

		next unless temp[9].to_s == eventId.to_s

		# Only add the related events that are new
		if relatedEvents[temp[0].to_s] == (0)
			newEvents.push(temp.join(","))
			relatedEvents[temp[0]] = 1
		end
	end

	return newEvents
end

# Recursive function that removes unnecessary nodes (ex. child at same Aiport as parent)
def verticalCondense(parentNode)
	condensed = false

	# Leaf nodes have no children to condense vertically
	return condensed if parentNode.is_leaf?

	# Recursive condese all nodes below current node
	parentNode.children.each do |childNode|
		condensed = verticalCondense(childNode) || condensed
	end

	parentTemp = parentNode.content.split(",")

	# Check child nodes to see if any can be condensed into current node
	parentNode.children.each do |childNode|

		childTemp = childNode.content.split(",")

		# If the parent event and the child event have the same
		# airport code, weather type, and severity
		# then some type of condensing is needed
		if parentTemp[8] == childTemp[8] && parentTemp[2] == childTemp[2] && parentTemp[3] == childTemp[3]
			condensed = true

			# If the end time for the child event is later than the parent event, update the event time
			if Time.parse(childTemp[5]) > Time.parse(parentTemp[5])
				parentTemp[5] = childTemp[5]
				parentNode.content = parentTemp.join(",")
			end

			# Grandkids are moved to become the children of the current node (Removes intermediate node)
			childNode.children.each do |grandchildNode|
				grandchildNode.remove_from_parent!
				parentNode << grandchildNode
			end

			# Delete child node. It should have no children and should have been absorbed
			childNode.remove_from_parent!
		end
	end

	return condensed
end

def horizontalCondense(parentNode)
	condensed = false

	# Recursive condese all nodes below current node
	parentNode.children.each do |childNode|
		condensed = horizontalCondense(childNode) || condensed
	end

	parentTemp = parentNode.content.split(",")

	# sibling nodes to see if any can acomidate the current node
	# If the parent node is the first sibling,no horizontal compression should be attempted
	parentNode.siblings.each do |siblingNode|

		siblingTemp = siblingNode.content.split(",")

		# If the parent event and the sibling event have the same
		# airport code, weather type, and severity
		# then some type of condensing is needed
		if parentTemp[8] == siblingTemp[8] && parentTemp[2] == siblingTemp[2] && parentTemp[3] == siblingTemp[3]
			condensed = true

			# NOTE: the current node is receiving from the sibling node

			# If the start time for the sibling event is earlier than the start time for the parent event, update the parent start time
			if Time.parse(siblingTemp[4]) < Time.parse(parentTemp[4])
				parentTemp[4] = siblingTemp[4]
				parentNode.content = parentTemp.join(",")
			end

			# If the end time for the sibling event is later than the end time for the parent event, update the parent end time
			if Time.parse(siblingTemp[5]) > Time.parse(parentTemp[5])
				siblingTemp[5] = parentTemp[5]
				siblingNode.content = siblingTemp.join(",")
			end

			# Children are moved from the sibling node to become the children of the parent node
			siblingNode.children.each do |childNode|
				childNode.remove_from_parent!
				parentNode << childNode
			end

			# Delete sibling node. It should have no children and should have been absorbed by the parent
			siblingNode.remove_from_parent!
		end

		return condensed
	end
end

def printStorm(printNode,fileName)
	outputFile = File.open(fileName, "a")
	printNode.print_tree(level = printNode.node_depth, max_depth = nil, block = lambda { |node, prefix|
		outputFile.write("#{prefix} #{node.content}\n")
	})
	outputFile.write("\n")
	outputFile.close
end

# Zero in ruby is true
def heuristic(parent, child, distanceRange)

	distance = Haversine.distance(parent[5].to_f, parent[6].to_f, child[5].to_f, child[6].to_f)
	if (distance.to_miles <= distanceRange) && (parent[5].to_f <= child[5].to_f) && (parent[6].to_f <= child[6].to_f)
		return 0
	else
		return 1
	end		
end

def sort()
	puts "Sorting"
	lines = CSV.read("WeatherData.csv")
	sortedLines = lines.sort_by{|line| Time.parse(line[4])}

	CSV.open("WeatherDataSorted.csv", 'wb') { |csv| sortedLines.each {|row| csv << row}}
end


# stripTrafficFile()
# sort()
# findStormStarts(14400, 100)

buildWeatherRelationHash()
buildStormStart(14400, 100)
