docker build -t mi911/data-collection-api . || exit 1
docker push mi911/data-collection-api
ssh roott@mindreader.tech "sh ~/reload_api.sh"
