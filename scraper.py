import requests
import json
import os
import time
import dotenv

dotenv.load_dotenv()


class MovieScraper:
    def __init__(self, tmdb_key, omdb_key):
        self.tmdb_key = tmdb_key
        self.omdb_key = omdb_key

        self.tmdb_base = "https://api.themoviedb.org/3"
        self.omdb_base = "http://www.omdbapi.com/"

        self.tmdb = requests.Session()
        self.tmdb.params = {"api_key": self.tmdb_key}

    def tmdb_get(self, endpoint, params=None):
        r = self.tmdb.get(f"{self.tmdb_base}{endpoint}", params=params)
        r.raise_for_status()
        return r.json()

    def discover_movies(self, start_year, end_year, pages_per_year=5):
        movie_ids = set()

        for year in range(start_year, end_year + 1):
            for page in range(1, pages_per_year + 1):
                data = self.tmdb_get(
                    "/discover/movie",
                    {
                        "primary_release_year": year,
                        "sort_by": "popularity.desc",
                        "vote_count.gte": 100,
                        "page": page,
                    },
                )
                print(f"Discovered page {page}, year {year}")

                for m in data.get("results", []):
                    movie_ids.add(m["id"])

                time.sleep(0.25)

        return list(movie_ids)

    def get_tmdb_record(self, movie_id):
        details = self.tmdb_get(f"/movie/{movie_id}")
        credits = self.tmdb_get(f"/movie/{movie_id}/credits")

        cast = sorted(
            credits.get("cast", []),
            key=lambda x: x.get("order", 99)
        )[:3]
        cast_pops = [c.get("popularity", 0) for c in cast]

        director_popularity = None
        for c in credits.get("crew", []):
            if c.get("job") == "Director":
                director_popularity = c.get("popularity")
                break

        return {
            "movie_id": movie_id,
            "title": details.get("title"),
            "release_date": details.get("release_date"),
            "budget": details.get("budget"),
            "revenue_worldwide": details.get("revenue"),
            "runtime": details.get("runtime"),
            "genres": [g["name"] for g in details.get("genres", [])],
            "imdb_id": details.get("imdb_id"),
            "franchise": details.get("belongs_to_collection") is not None,
            "cast_popularity_mean": (
                sum(cast_pops) / len(cast_pops) if cast_pops else None
            ),
            "cast_popularity_max": max(cast_pops) if cast_pops else None,
            "director_popularity": director_popularity,
            "original_language": details.get("original_language"),
        }

    def get_omdb_record(self, imdb_id):
        if not imdb_id:
            return {}

        params = {
            "apikey": self.omdb_key,
            "i": imdb_id,
            "plot": "short",
        }

        r = requests.get(self.omdb_base, params=params)
        if r.status_code != 200:
            return {}

        data = r.json()
        if data.get("Response") != "True":
            return {}

        rt_score = None
        mc_score = None
        for r in data.get("Ratings", []):
            if r["Source"] == "Rotten Tomatoes":
                rt_score = r["Value"].replace("%", "")
            elif r["Source"] == "Metacritic":
                mc_score = r["Value"].split("/")[0]

        return {
            "imdb_rating": float(data["imdbRating"])
                if data.get("imdbRating") not in ("N/A", None) else None,
            "imdb_votes": int(data["imdbVotes"].replace(",", ""))
                if data.get("imdbVotes") not in ("N/A", None) else None,
            "mpaa_rating": data.get("Rated"),
            "domestic_box_office": (
                int(data["BoxOffice"].replace("$", "").replace(",", ""))
                if data.get("BoxOffice") not in ("N/A", None) else None
            ),
            "rotten_tomatoes_score": (
                int(rt_score) if rt_score else None
            ),
            "metacritic_score": (
                int(mc_score) if mc_score else None
            ),
            "awards_text": data.get("Awards"),
        }

    def scrape(self, start_year=2000, end_year=2025, pages_per_year=15):
        movie_ids = self.discover_movies(start_year, end_year, pages_per_year=pages_per_year)

        os.makedirs("data", exist_ok=True)
        out_path = "data/raw.jsonl"

        with open(out_path, "a", encoding="utf-8") as f:
            for i, movie_id in enumerate(movie_ids, 1):
                try:
                    tmdb_data = self.get_tmdb_record(movie_id)
                    omdb_data = self.get_omdb_record(tmdb_data.get("imdb_id"))

                    record = {**tmdb_data, **omdb_data}
                    f.write(json.dumps(record) + "\n")

                    print(f"Scraped movie {i}")

                    time.sleep(0.5)

                except Exception as e:
                    print(f"Failed movie_id={movie_id}: {e}")

        print("Scraping complete.")


if __name__ == "__main__":
    scraper = MovieScraper(
        tmdb_key=os.getenv("TMDB_API_KEY"),
        omdb_key=os.getenv("OMDB_API_KEY"),
    )
    scraper.scrape()

