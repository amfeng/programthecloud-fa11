import web
        
urls = (
    '/member', 'member',
    '/vote', 'vote',
    '/victor', 'victor',
    '/rst', 'rst'
)
app = web.application(urls, globals())

# Global variables
members = {}
alreadyVoted = {}
votes = {}
numVoters = 0
votingStarted = False

def member_exists(name):
  return name in members

class member:
  def POST(self):
    global members, numVoters
    # Register agent name
    agent = web.input().agent
    if agent not in members and not votingStarted: 
      members[agent] = agent    
      numVoters += 1

class vote:
  def POST(self):
    global votingStarted , alreadyVoted, votes
    # Cast a vote for an agent
    params = web.input()
    agent = params.agent
    votedFor = params.vote
    if member_exists(agent) and agent not in alreadyVoted:
      votingStarted = True
      alreadyVoted[agent] = votedFor
      votes[votedFor] = votes.setdefault(votedFor, 0) + 1

class victor:        
  def GET(self):
    global votes
    # Check if anyone has won the election
    # TODO
    if (len(votes) > 0): 
      threshold = numVoters / 2
      sortedList = sorted(votes, key=lambda key: votes[key])
      sortedList.reverse()
      winner = sortedList[0]
      if (votes[winner] > threshold):
        return winner
    return "UNKNOWN"

class rst:
  def POST(self):
    # Reinitialize the server state
    members = {}
    alreadyVoted = {}
    votes = {}
    numVoters = 0
    votingStarted = False

if __name__ == "__main__":
    app.run()
